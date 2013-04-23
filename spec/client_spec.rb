require File.dirname(__FILE__) + '/../lib/sensu/client.rb'
require File.dirname(__FILE__) + '/helpers.rb'

describe 'Sensu::Client' do
  include Helpers

  before do
    @client = Sensu::Client.new(options)
  end

  it 'can connect to rabbitmq' do
    async_wrapper do
      @client.setup_rabbitmq
      async_done
    end
  end

  it 'can send a keepalive' do
    async_wrapper do
      @client.setup_rabbitmq
      @client.publish_keepalive
      amq.queue('keepalives').subscribe do |headers, payload|
        keepalive = JSON.parse(payload, :symbolize_names => true)
        keepalive[:name].should eq('i-424242')
        async_done
      end
    end
  end

  it 'can schedule keepalive publishing' do
    async_wrapper do
      @client.setup_rabbitmq
      @client.setup_keepalives
      amq.queue('keepalives').subscribe do |headers, payload|
        keepalive = JSON.parse(payload, :symbolize_names => true)
        keepalive[:name].should eq('i-424242')
        async_done
      end
    end
  end

  it 'can send a check result' do
    async_wrapper do
      @client.setup_rabbitmq
      check = result_template[:check]
      @client.publish_result(check)
      amq.queue('results').subscribe do |headers, payload|
        result = JSON.parse(payload, :symbolize_names => true)
        result[:client].should eq('i-424242')
        result[:check][:name].should match /foobar/
        async_done
      end
    end
  end

  it 'can execute a check' do
    async_wrapper do
      @client.setup_rabbitmq
      @client.execute_check(check_template)
      amq.queue('results').subscribe do |headers, payload|
        result = JSON.parse(payload, :symbolize_names => true)
        result[:client].should eq('i-424242')
        result[:check][:output].should match /WARNING/
        async_done
      end
    end
  end

  it 'can execute a ruby check in a forked process' do
    async_wrapper do
      @client.setup_rabbitmq
      @client.execute_check(check_template.merge({:command => "#{File.dirname(__FILE__)}/ruby_fork_test.rb \"arg list\" of things \"^reg ex$\"", :fork => true}))
      amq.queue('results').subscribe do |headers, payload|
        result = JSON.parse(payload, :symbolize_names => true)
        result[:client].should eq('i-424242')
        result[:check][:output].should eq('ruby forked process with 4 args ["arg list", "of", "things", "^reg ex$"]')
        async_done
      end
    end
  end

  it 'supports timeouts for hanging processes' do
    lambda { @client.execute_with_ruby_fork("#{File.dirname(__FILE__)}/ruby_fork_test.rb \"arg list\" of things \"^reg ex$\"", 1) }.should raise_error Timeout::Error
    lambda { @client.execute_with_ruby_fork("#{File.dirname(__FILE__)}/ruby_fork_test.rb \"arg list\" of things \"^reg ex$\"", 3) }.should_not raise_error
    @client.execute_with_ruby_fork("#{File.dirname(__FILE__)}/ruby_fork_test.rb \"arg list\" of things \"^reg ex$\"", 3).should =~ ["ruby forked process with 4 args [\"arg list\", \"of\", \"things\", \"^reg ex$\"]", 0]
  end


  it 'can substitute check command tokens with attributes and execute it' do
    async_wrapper do
      @client.setup_rabbitmq
      check = check_template
      check[:command] = 'echo -n :::nested.attribute:::'
      @client.execute_check(check)
      amq.queue('results').subscribe do |headers, payload|
        result = JSON.parse(payload, :symbolize_names => true)
        result[:client].should eq('i-424242')
        result[:check][:output].should match /true/
        async_done
      end
    end
  end

  it 'can substitute check command tokens with settings prefix with attributes in full settings and execute it' do
    async_wrapper do
      @client.setup_rabbitmq
      check = check_template
      check[:command] = 'echo -n :::settings.api.port:::'
      @client.execute_check(check)
      amq.queue('results').subscribe do |headers, payload|
        result = JSON.parse(payload, :symbolize_names => true)
        result[:client].should eq('i-424242')
        result[:check][:output].should include('4567')
        async_done
      end
    end
  end


  it 'can setup subscriptions' do
    async_wrapper do
      @client.setup_rabbitmq
      @client.setup_subscriptions
      timer(1) do
        amq.fanout('test', :passive => true) do |exchange, declare_ok|
          declare_ok.should be_an_instance_of(AMQ::Protocol::Exchange::DeclareOk)
          exchange.status.should eq(:opening)
          async_done
        end
      end
    end
  end

  it 'can receive a check request and execute the check' do
    async_wrapper do
      @client.setup_rabbitmq
      @client.setup_subscriptions
      timer(1) do
        amq.fanout('test').publish(check_template.to_json)
      end
      amq.queue('results').subscribe do |headers, payload|
        result = JSON.parse(payload, :symbolize_names => true)
        result[:client].should eq('i-424242')
        result[:check][:output].should match /WARNING/
        result[:check][:status].should eq(1)
        async_done
      end
    end
  end

  it 'can receive a check request and not execute the check due to safe mode' do
    async_wrapper do
      @client.safe_mode = true
      @client.setup_rabbitmq
      @client.setup_subscriptions
      timer(1) do
        amq.fanout('test').publish(check_template.to_json)
      end
      amq.queue('results').subscribe do |headers, payload|
        result = JSON.parse(payload, :symbolize_names => true)
        result[:client].should eq('i-424242')
        result[:check][:output].should include('safe mode')
        result[:check][:status].should eq(3)
        async_done
      end
    end
  end

  it 'can schedule standalone check execution' do
    async_wrapper do
      @client.setup_rabbitmq
      @client.setup_standalone
      amq.queue('results').subscribe do |headers, payload|
        result = JSON.parse(payload, :symbolize_names => true)
        result[:client].should eq('i-424242')
        result[:check][:name].should eq('standalone')
        result[:check][:output].should match /foobar/
        result[:check][:status].should eq(1)
        async_done
      end
    end
  end

  it 'can accept external result input via sockets' do
    async_wrapper do
      @client.setup_rabbitmq
      @client.setup_sockets
      timer(1) do
        EM::connect('127.0.0.1', 3030, nil) do |socket|
          socket.send_data('{"name": "tcp", "output": "tcp", "status": 1}')
          socket.close_connection_after_writing
        end
        EM::open_datagram_socket('127.0.0.1', 0, nil) do |socket|
          data = '{"name": "udp", "output": "udp", "status": 1}'
          socket.send_datagram(data, '127.0.0.1', 3030)
          socket.close_connection_after_writing
        end
      end
      expected = ['tcp', 'udp']
      amq.queue('results').subscribe do |headers, payload|
        result = JSON.parse(payload, :symbolize_names => true)
        result[:client].should eq('i-424242')
        expected.delete(result[:check][:name]).should_not be_nil
        if expected.empty?
          async_done
        end
      end
    end
  end

  after(:all) do
    async_wrapper do
      amq.queue('results').purge do
        amq.queue('keepalives').purge do
          async_done
        end
      end
    end
  end
end
