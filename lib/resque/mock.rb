require 'resque'
require 'resque_scheduler'

module ResqueScheduler
  def self.foo(m)
    remove_method m.name
  end

  def self.unfoo(m)
    define_method(m.name, m)
  end
end

module Resque
  def self.mock!
    @old_enqueue = method(:enqueue)
    @old_sleep = method(:sleep)
    define_method(:sleep) { |seconds| Kernel.sleep seconds }
    define_method(:async) { |&block| MockExt.async(&block) }
    define_method(:enqueue) { |klass, *args| MockExt.enqueue(klass, *args) }

    if methods.include?(:enqueue_in)
      @old_enqueue_in = method(:enqueue_in)
      ResqueScheduler.foo @old_enqueue_in
      define_method(:enqueue_in) { |delay, klass, *args| MockExt.enqueue_in(delay, klass, *args) }
    end
  end

  def self.unmock!
    raise "It's not mocked!" if @old_enqueue.nil? || @old_sleep.nil?

    remove_method :sleep
    define_method(:sleep, &@old_sleep)
    @old_sleep = nil

    remove_method :enqueue
    define_method(:enqueue, &@old_enqueue)
    @old_enqueue = nil

    remove_method :enqueue_in rescue puts "couldn't remove :enqueue_in"
    ResqueScheduler.unfoo @old_enqueue_in

    remove_method :async
  end

  module MockExt
    class << self
      def async
        @async = true
        create_worker_manager
        yield
      ensure
        wait_for_worker_manager
        @async = false
      end

      def enqueue(klass, *args)
        puts "Mock enqueue: async=#{!!@async}, stack_depth=#{caller.size}, #{klass}, #{args.inspect}" if ENV['VERBOSE']
        defer(klass, args, nil)
      end

      def enqueue_in(delay, klass, *args)
        puts "Mock enqueue in #{delay}: async=#{!!@async}, stack_depth=#{caller.size}, #{klass}, #{args.inspect}" if ENV['VERBOSE']
        defer(klass, args, delay)
      end

      def defer(klass, args, delay)
        Resque.validate(klass)

        if @async
          add_job('payload' => { 'class' => klass, 'args' => args }, 'delay' => delay)
        else
          Resque.sleep delay if delay
          klass.perform(*roundtrip(args))
        end
      end

      def create_worker_manager
        @worker_manager = Thread.new do
          Thread.current.abort_on_exception = true
          worker_threads = []

          while true
            break if Thread.current[:exit] && worker_threads.empty? && Thread.current[:jobs].empty?

            worker_threads.reject! {|t| !t.alive? }

            while Thread.current[:jobs] && job_data = Thread.current[:jobs].shift
              worker_threads << create_worker_thread_for(job_data)
            end

            Resque.sleep 0.5
          end
        end.tap {|t| t[:jobs] = [] }
      end

      def wait_for_worker_manager
        @worker_manager[:exit] = true
        @worker_manager.join
        @worker_manager = nil
      end

      def create_worker_thread_for(data)
        Thread.new(data) do |data|
          Thread.current.abort_on_exception = true
          if delay = data['delay']
            Resque.sleep delay
          end

          klass = data['payload']['class']
          puts "Mock perform: #{klass}.perform(*#{data['payload']['args'].inspect})" if ENV['VERBOSE']
            klass.perform(*roundtrip(data['payload']['args']))
            puts "Mock exit: #{klass}.perform(*#{data['payload']['args'].inspect})" if ENV['VERBOSE']
        end
      end

      def roundtrip(args)
        Resque.decode(Resque.encode(args))
      end

      def add_job(data)
        @worker_manager[:jobs] << data
      end
    end
  end
end
