require File.join(File.dirname(__FILE__), 'base')
require File.join(File.dirname(__FILE__), 'socket')
require 'timeout'

module Sensu
  class Client
    include Utilities

    attr_accessor :safe_mode

    def self.run(options={})
      client = self.new(options)
      EM::run do
        client.start
        client.trap_signals
      end
    end

    def initialize(options={})
      base = Base.new(options)
      @logger = base.logger
      @settings = base.settings
      @extensions = base.extensions
      base.setup_process
      @timers = Array.new
      @checks_in_progress = Array.new
      @safe_mode = @settings[:client][:safe_mode] || false
    end

    def setup_rabbitmq
      @logger.debug('connecting to rabbitmq', {
        :settings => @settings[:rabbitmq]
      })
      @rabbitmq = RabbitMQ.connect(@settings[:rabbitmq])
      @rabbitmq.on_error do |error|
        @logger.fatal('rabbitmq connection error', {
          :error => error.to_s
        })
        stop
      end
      @rabbitmq.before_reconnect do
        @logger.warn('reconnecting to rabbitmq')
      end
      @rabbitmq.after_reconnect do
        @logger.info('reconnected to rabbitmq')
      end
      @amq = @rabbitmq.channel
    end

    def publish_keepalive
      payload = @settings[:client].merge(:timestamp => Time.now.to_i)
      @logger.debug('publishing keepalive', {
        :payload => payload
      })
      @amq.direct('keepalives').publish(Oj.dump(payload))
    end

    def setup_keepalives
      @logger.debug('scheduling keepalives')
      publish_keepalive
      @timers << EM::PeriodicTimer.new(20) do
        if @rabbitmq.connected?
          publish_keepalive
        end
      end
    end

    def publish_result(check)
      payload = {
        :client => @settings[:client][:name],
        :check => check
      }
      @logger.info('publishing check result', {
        :payload => payload
      })
      @amq.direct('results').publish(Oj.dump(payload))
    end

    def substitute_command_tokens(check)
      unmatched_tokens = Array.new
      substituted = check[:command].gsub(/:::(.*?):::/) do
        token_fields = $1.to_s.split('.')
        # if token is :::settings.something::: look in full settings hash
        if token_fields[0] == 'settings' 
          config = @settings
          token_fields = token_fields[1..-1]
        end        
        matched = token_fields.inject(@settings[:client]) do |client, attribute|
          client[attribute].nil? ? break : client[attribute]
        end
        if matched.nil?
          unmatched_tokens << token
        end
        matched
      end
      [substituted, unmatched_tokens]
    end

    # if the command is a ruby script it is around 20 times faster to fork the client
    # process than to sh -c ruby the script and it doesn't spike the CPU nearly as much
    # check_timeout is set by configuring check[:timeout] in the check json
    def execute_with_ruby_fork(command, check_timeout=60)
      begin
        # set this so we can access it later
        child_pid = nil
        # wrap the whole thing in the timeout
        timeout(check_timeout) do
          @logger.debug('attempting to execute command in ruby fork', {
            :command => command
          })
          
          # IO.popen does not run at_exit handlers which is what sensu-plugins use to run scripts
          # So we have to do the output capture manually.
          rd, wr = ::IO.pipe
          child_pid = ::Kernel.fork do
            rd.close
            STDOUT.reopen(wr)
            file, *args = Shellwords.split(command)
            ARGV.replace(args)
            load(file,true)
          end
          wr.close
          output = rd.read
          rd.close
        
          IO.send(:wait_on_process_group, child_pid)
        end
      rescue Timeout::Error => e
        @logger.warn('command timed out; process will be terminated', {
          :command => command,
          :timeout => check_timeout,
          :pid => child_pid
        })
        IO.send(:kill_process_group, child_pid)
        raise e
      end
    end

    def fork_ruby_check?(check)
      check[:fork] || ( @settings[:client][:fork_ruby_checks] && check[:command].split[0].end_with?(".rb") )
    end

    def execute_check(check)
      @logger.debug('attempting to execute check', {
        :check => check
      })
      unless @checks_in_progress.include?(check[:name])
        @logger.debug('executing check', {
          :check => check
        })
        @checks_in_progress << check[:name]
        
        command, unmatched_tokens = substitute_command_tokens(check)
        check[:executed] = Time.now.to_i

        if unmatched_tokens.empty?
          execute = Proc.new do
            @logger.debug('executing check command', {
              :check => check
            })
            started = Time.now.to_f
            begin
              check[:output], check[:status] = if fork_ruby_check?(check)
                                                 execute_with_ruby_fork(command, check[:timeout])
                                               else
                                                 IO.popen(command, 'r', check[:timeout])
                                               end
            rescue => error
              @logger.warn('unexpected error', {
                :error => error.to_s,
                :command => command
              })
              check[:output] = 'Unexpected error: ' + error.to_s
              check[:status] = 3
            end
            check[:duration] = ('%.3f' % (Time.now.to_f - started)).to_f
            check
          end
          publish = Proc.new do |check|
            publish_result(check)
            @checks_in_progress.delete(check[:name])
          end
          EM::defer(execute, publish)
        else
          check[:output] = 'Unmatched command tokens: ' + unmatched_tokens.join(', ')
          check[:status] = 3
          check[:handle] = false
          publish_result(check)
          @checks_in_progress.delete(check[:name])
        end
      else
        @logger.warn('previous check command execution in progress', {
          :check => check
        })
      end
    end

    def run_check_extension(check)
      @logger.debug('attempting to run check extension', {
        :check => check
      })
      check[:executed] = Time.now.to_i
      extension = @extensions[:checks][check[:name]]
      extension.run do |output, status|
        check[:output] = output
        check[:status] = status
        publish_result(check)
      end
    end

    def process_check(check)
      @logger.debug('processing check', {
        :check => check
      })
      if check.has_key?(:command)
        if @settings.check_exists?(check[:name])
          check.merge!(@settings[:checks][check[:name]])
          execute_check_command(check)
        elsif @safe_mode
          check[:output] = 'Check is not locally defined (safe mode)'
          check[:status] = 3
          check[:handle] = false
          check[:executed] = Time.now.to_i
          publish_result(check)
        else
          execute_check_command(check)
        end
      else
        if @extensions.check_exists?(check[:name])
          run_check_extension(check)
        else
          @logger.warn('unknown check extension', {
            :check => check
          })
        end
      end
    end

    def setup_subscriptions
      @logger.debug('subscribing to client subscriptions')
      @check_request_queue = @amq.queue('', :auto_delete => true) do |queue|
        @settings[:client][:subscriptions].each do |exchange_name|
          @logger.debug('binding queue to exchange', {
            :queue_name => queue.name,
            :exchange_name => exchange_name
          })
          queue.bind(@amq.fanout(exchange_name))
        end
        queue.subscribe do |payload|
          begin
            check = Oj.load(payload)
            @logger.info('received check request', {
              :check => check
            })
            process_check(check)
          rescue Oj::ParseError => error
            @logger.warn('check request payload must be valid json', {
              :payload => payload,
              :error => error.to_s
            })
          end
        end
      end
    end

    def schedule_checks(checks)
      check_count = 0
      stagger = testing? ? 0 : 2
      checks.each do |check|
        check_count += 1
        scheduling_delay = stagger * check_count % 30
        @timers << EM::Timer.new(scheduling_delay) do
          interval = testing? ? 0.5 : check[:interval]
          @timers << EM::PeriodicTimer.new(interval) do
            if @rabbitmq.connected?
              check[:issued] = Time.now.to_i
              process_check(check)
            end
          end
        end
      end
    end

    def setup_standalone
      @logger.debug('scheduling standalone checks')
      standard_checks = @settings.checks.select do |check|
        check[:standalone]
      end
      extension_checks = @extensions.checks.select do |check|
        check[:standalone] && check[:interval].is_a?(Integer)
      end
      schedule_checks(standard_checks + extension_checks)
    end

    def setup_sockets
      @logger.debug('binding client tcp socket')
      EM::start_server('127.0.0.1', 3030, Socket) do |socket|
        socket.logger = @logger
        socket.settings = @settings
        socket.amq = @amq
      end
      @logger.debug('binding client udp socket')
      EM::open_datagram_socket('127.0.0.1', 3030, Socket) do |socket|
        socket.logger = @logger
        socket.settings = @settings
        socket.amq = @amq
        socket.reply = false
      end
    end

    def unsubscribe
      @logger.warn('unsubscribing from client subscriptions')
      if @rabbitmq.connected?
        @check_request_queue.unsubscribe
      else
        @check_request_queue.before_recovery do
          @check_request_queue.unsubscribe
        end
      end
    end

    def complete_checks_in_progress(&block)
      @logger.info('completing checks in progress', {
        :checks_in_progress => @checks_in_progress
      })
      retry_until_true do
        if @checks_in_progress.empty?
          block.call
          true
        end
      end
    end

    def start
      setup_rabbitmq
      setup_keepalives
      setup_subscriptions
      setup_standalone
      setup_sockets
    end

    def stop
      @logger.warn('stopping')
      @timers.each do |timer|
        timer.cancel
      end
      unsubscribe
      complete_checks_in_progress do
        @extensions.stop_all do
          @rabbitmq.close
          @logger.warn('stopping reactor')
          EM::stop_event_loop
        end
      end
    end

    def trap_signals
      @signals = Array.new
      STOP_SIGNALS.each do |signal|
        Signal.trap(signal) do
          @signals << signal
        end
      end
      EM::PeriodicTimer.new(1) do
        signal = @signals.shift
        if STOP_SIGNALS.include?(signal)
          @logger.warn('received signal', {
            :signal => signal
          })
          stop
        end
      end
    end
  end
end
