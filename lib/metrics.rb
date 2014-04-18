require 'socket'
require 'forwardable'
require 'json'

# = Metrics: A metricsd client (https://github.com/premisedata/metricsd)
#
# @example Set up a global Metrics for a server on localhost:8125
#   $metrics = Metrics.new 'localhost', 8125
# @example Set up a global Metrics for a server on IPv6 port 8125
#   $metrics = Metrics.new '::1', 8125
# @example Send some stats
#   $metrics.increment 'garets'
#   $metrics.timer 'glork', 320
#   $metrics.gauge 'bork', 100
# @example Use {#timed} to time the execution of a block
#   $metrics.timed('account.activate') { @account.activate! }
# @example Create a namespaced Metrics and increment 'account.activate'
#   metrics = Metrics.new('localhost').tap{|sd| sd.namespace = 'account'}
#   metrics.increment 'activate'
#
# Metrics instances are thread safe for general usage, by using a thread local
# UDPSocket and carrying no state. The attributes are stateful, and are not
# mutexed, it is expected that users will not change these at runtime in
# threaded environments. If users require such use cases, it is recommend that
# users either mutex around their Metrics object, or create separate objects for
# each namespace / host+port combination.
class Metrics

  # = Batch: A batching Metrics proxy
  #
  # @example Batch a set of instruments using Batch and manual flush:
  #   $metrics = Metrics.new 'localhost', 8125
  #   batch = Metrics::Batch.new($metrics)
  #   batch.increment 'garets'
  #   batch.timer 'glork', 320
  #   batch.gauge 'bork', 100
  #   batch.flush
  #
  # Batch is a subclass of Metrics, but with a constructor that proxies to a
  # normal Metrics instance. It has it's own batch_size and namespace parameters
  # (that inherit defaults from the supplied Metrics instance). It is recommended
  # that some care is taken if setting very large batch sizes. If the batch size
  # exceeds the allowed packet size for UDP on your network, communication
  # troubles may occur and data will be lost.
  class Batch < Metrics

    extend Forwardable
    def_delegators :@metrics,
      :namespace, :namespace=,
      :host, :host=,
      :port, :port=,
      :prefix,
      :postfix

    attr_accessor :batch_size

    # @param [Metrics] requires a configured Metrics instance
    def initialize(metrics)
      @metrics = metrics
      @batch_size = metrics.batch_size
      @backlog = []
    end

    # @yields [Batch] yields itself
    #
    # A convenience method to ensure that data is not lost in the event of an
    # exception being thrown. Batches will be transmitted on the parent socket
    # as soon as the batch is full, and when the block finishes.
    def easy
      yield self
    ensure
      flush
    end

    def flush
      unless @backlog.empty?
        @metrics.send_to_socket @backlog.join("\n")
        @backlog.clear
      end
    end

    protected

    def send_to_socket(message)
      @backlog << message
      if @backlog.size >= @batch_size
        flush
      end
    end

  end

  class Admin
    # metricsd host. Defaults to 127.0.0.1.
    attr_reader :host

    # metricsd admin port. Defaults to 8126.
    attr_reader :port

    class << self
      # Set to a standard logger instance to enable debug logging.
      attr_accessor :logger
    end

    # @attribute [w] host
    #   Writes are not thread safe.
    def host=(host)
      @host = host || '127.0.0.1'
    end

    # @attribute [w] port
    #   Writes are not thread safe.
    def port=(port)
      @port = port || 8126
    end

    # @param [String] host your metricsd host
    # @param [Integer] port your metricsd port
    def initialize(host = '127.0.0.1', port = 8126)
      self.host, self.port = host, port
    end

    # Reads all gauges from metricsd.
    def gauges
      read_metric :gauges
    end

    # Reads all timers from metricsd.
    def timers
      read_metric :timers
    end

    # Reads all counters from metricsd.
    def counters
      read_metric :counters
    end

    # @param[String] item
    #   Deletes one or more gauges. Wildcards are allowed.
    def delgauges item
      delete_metric :gauges, item
    end

    # @param[String] item
    #   Deletes one or more timers. Wildcards are allowed.
    def deltimers item
      delete_metric :timers, item
    end

    # @param[String] item
    #   Deletes one or more counters. Wildcards are allowed.
    def delcounters item
      delete_metric :counters, item
    end

    def stats
      # the format of "stats" isn't JSON, who knows why
      send_to_socket "stats"
      result = read_from_socket
      items = {}
      result.split("\n").each do |line|
        key, val = line.chomp.split(": ")
        items[key] = val.to_i
      end
      items
    end

    private

    def read_metric name
      send_to_socket name
      result = read_from_socket
      # for some reason, the reply looks like JSON, but isn't, quite
      JSON.parse result.gsub("'", "\"")
    end

    def delete_metric name, item
      send_to_socket "del#{name} #{item}"
      result = read_from_socket
      deleted = []
      result.split("\n").each do |line|
        deleted << line.chomp.split(": ")[-1]
      end
      deleted
    end

    def send_to_socket(message)
      self.class.logger.debug { "Metrics: #{message}" } if self.class.logger
      socket.write(message.to_s + "\n")
    rescue => boom
      self.class.logger.error { "Metrics: #{boom.class} #{boom}" } if self.class.logger
      nil
    end


    def read_from_socket
      buffer = ""
      loop do
        line = socket.readline
        break if line == "END\n"
        buffer += line
      end
      socket.readline # clear the closing newline out of the socket
      buffer
    end

    def socket
      Thread.current[:metricsd_admin_socket] ||= TCPSocket.new(host, port)
    end
  end

  # A namespace to prepend to all metricsd calls.
  attr_reader :namespace

  # metricsd host. Defaults to 127.0.0.1.
  attr_reader :host

  # metricsd port. Defaults to 8125.
  attr_reader :port

  # metricsd namespace prefix, generated from #namespace
  attr_reader :prefix

  # The default batch size for new batches (default: 10)
  attr_accessor :batch_size

  # a postfix to append to all metrics
  attr_reader :postfix

  class << self
    # Set to a standard logger instance to enable debug logging.
    attr_accessor :logger
  end

  # @param [String] host your metricsd host
  # @param [Integer] port your metricsd port
  def initialize(host = '127.0.0.1', port = 8125)
    self.host, self.port = host, port
    @prefix = nil
    @batch_size = 10
    @postfix = nil
  end

  # @attribute [w] namespace
  #   Writes are not thread safe.
  def namespace=(namespace)
    @namespace = namespace
    @prefix = "#{namespace}."
  end

  # @attribute [w] postfix
  #   A value to be appended to the stat name after a '.'. If the value is
  #   blank then the postfix will be reset to nil (rather than to '.').
  def postfix=(pf)
    case pf
    when nil, false, '' then @postfix = nil
    else @postfix = ".#{pf}"
    end
  end

  # @attribute [w] host
  #   Writes are not thread safe.
  def host=(host)
    @host = host || '127.0.0.1'
  end

  # @attribute [w] port
  #   Writes are not thread safe.
  def port=(port)
    @port = port || 8125
  end

  # Set a gauge's value. The gauge value persists until the next time it's set.
  # [http://metrics.codahale.com/manual/core/#man-core-gauges]
  def gauge(stat, value, sample_rate=1)
    send_stats stat, value, :g, sample_rate
  end

  # Increment/decrement a counter. A counter is a gauge that you update with a
  # relative offset instead of an absolute new value.
  # [http://metrics.codahale.com/manual/core/#man-core-counters]
  def count(stat, offset, sample_rate=1)
    send_stats stat, offset, :c, sample_rate
  end
  def increment(stat, sample_rate=1)
    count stat, 1, sample_rate
  end
  def decrement(stat, sample_rate=1)
    count stat, -1, sample_rate
  end

  # Mark a meter. Meters track the rate at which some event occurs.
  # [http://metrics.codahale.com/manual/core/#man-core-meters]
  def meter(stat, sample_rate=1)
    send_stats stat, nil, nil, sample_rate
  end

  # Report a histogram sample. Histograms track the following distribution stats
  # using constant space: 50p, 75p, 98p, 99p, 99.9p, min, max, mean, stddev.
  # [http://metrics.codahale.com/manual/core/#man-core-histograms]
  def histo(stat, value, sample_rate=1)
    send_stats stat, value, :h, sample_rate
  end

  # Report a timer sample in millis. A timer is a histogram of millis plus a
  # meter that's marked on each report.
  # [http://metrics.codahale.com/manual/core/#man-core-timers]
  def timer(stat, millis, sample_rate=1)
    send_stats stat, millis, :ms, sample_rate
  end

  # Report the running time of the provided block using {#timer}.
  def timed(stat, sample_rate=1)
    start = Time.now
    result = yield
    timer(stat, ((Time.now - start) * 1000).round, sample_rate)
    result
  end

  # Creates and yields a Batch that can be used to batch instrument reports into
  # larger packets. Batches are sent either when the packet is "full" (defined
  # by batch_size), or when the block completes, whichever is the sooner.
  #
  # @yield [Batch] a Metrics subclass that collects and batches instruments
  # @example Batch two instument operations:
  #   $metrics.batch do |batch|
  #     batch.increment 'sys.requests'
  #     batch.gauge('user.count', User.count)
  #   end
  def batch(&block)
    Batch.new(self).easy &block
  end

  protected

  def send_to_socket(message)
    self.class.logger.debug { "Metrics: #{message}" } if self.class.logger
    socket.send(message, 0, @host, @port)
  rescue => boom
    self.class.logger.error { "Metrics: #{boom.class} #{boom}" } if self.class.logger
    nil
  end

  private

  def send_stats(stat, value, type, sample_rate=1)
    raise ArgumentError, "value must be Integer or nil, got: #{value} (#{value.class})" unless value.is_a? Integer or value == nil
    if sample_rate == 1 or rand < sample_rate
      # Replace Ruby module scoping with '.' and reserved chars (: | @) with underscores.
      stat  = stat.to_s.gsub('::', '.').tr(':|@', '_')
      rate  = "|@#{sample_rate}" unless sample_rate == 1
      value = ":#{value}" if value
      type  = "|#{type}"  if type
      send_to_socket "#{prefix}#{stat}#{postfix}#{value}#{type}#{rate}"
    end
  end

  def socket
    Thread.current[:metricsd_socket] ||= UDPSocket.new addr_family
  end

  def addr_family
    Addrinfo.udp(@host, @port).ipv6? ? Socket::AF_INET6 : Socket::AF_INET
  end
end
