# Metricstore

Metrics in a key-value store.

Accepts, summarizes, and stores continuous metrics updates to a key-value store.
Answers queries in constant time.

We assume that the backing key-value store is fast, durable, and supports TTL.
Initial implementation will use Couchbase.

## Installation

Follow the instructions at https://github.com/couchbase/couchbase-ruby-client
to install the couchbase gem.

Add this line to your application's Gemfile:

    gem 'metricstore'

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install metricstore

## Usage

    m = Metricstore.couchbase(:hostname => "localhost")

    # Configure...
    m.ttl_of_hours = 31_556_926 # 1 year, default
    m.max_ttl_of_dimension[:session_id] = 7200 # 2 hours
    m.list_threshold = 1000 # default

    # Open the connection
    m.open

    # (Suppose that the current time is 17:05 UTC on April 13, 2012.)

    m.counter(:when => Time.now, :what => "logins", :where =>
     {:user => 'joe', :ip => '10.20.30.40'})
    m.counter(:when => Time.now, :what => "logins", :where =>
     {:user => 'bob', :ip => '10.20.30.40'})
    m.counter(:when => Time.now, :what => "logins", :where =>
     {:user => 'joe', :ip => '10.20.30.50'})

    m.measure(:when => Time.now, :what => "load_time", :value => 340, :where =>
     {:page => '/welcome/', :session_id => "h0zhmb1c-u1xfgw305e"})
    m.measure(:when => Time.now, :what => "load_time", :value => 501, :where =>
     {:page => '/welcome/', :session_id => "h0zhmb2q-643dotlcgd"})
    m.measure(:when => Time.now, :what => "load_time", :value => 212, :where =>
     {:page => '/welcome/', :session_id => "h0zhmb1c-u1xfgw305e"})
    m.measure(:when => Time.now, :what => "load_time", :value => 343, :where =>
     {:page => '/welcome/', :session_id => "h0zhmb2q-643dotlcgd"})

    # Now we can query...

    m.count(:hour => "2012-04-13-17", :what => "logins")
     => 3
    m.list(:hour => "2012-04-13-17", :what => "logins", :list => :user)
     => ['joe', 'bob']
    m.count(:hour => "2012-04-13-17", :what => "logins", :where => {:user => 'joe'})
     => 2
    m.count(:hour => "2012-04-13-17", :what => "logins", :where => {:user => 'bob'})
     => 1
    m.list(:hour => "2012-04-13-17", :what => "logins", :where => {:user => 'joe'}, :list => :ip)
     => ['10.20.30.40', '10.20.30.50']
    m.list(:hour => "2012-04-13-17", :what => "logins", :where => {:user => 'bob'}, :list => :ip)
     => ['10.20.30.40']
    m.count(:hour => "2012-04-13-17", :what => "logins", :where => {:user => 'joe', :ip => '10.20.30.40'})
     => 1

    m.count(:hour => "2012-04-13-17", :what => "load_time")
     => 4
    m.sum(:hour => "2012-04-13-17", :what => "load_time")
     => 1396
    m.average(:hour => "2012-04-13-17", :what => "load_time")
     => 349.0
    m.maximum(:hour => "2012-04-13-17", :what => "load_time")
     => 501
    m.minimum(:hour => "2012-04-13-17", :what => "load_time")
     => 212
    m.stddev(:hour => "2012-04-13-17", :what => "load_time")
     => 102.45730818248154
    m.list(:hour => "2012-04-13-17", :what => "load_time", :list => :page)
     => ['/welcome/']

    # We can do queries related to groups as well, with some limitations.
    # We only guarantee the accuracy of a particular group summary if for every
    # member in the group, all the metrics related to that member were loaded
    # from start-to-finish before the preceeding such metric expired its TTL.
    #
    # Note: a range is the difference between the minimum and maximum metric,
    # for an individual group.

    m.count_of_groups(:hour => "2012-04-13-17", :what => "load_time", :group => :session_id)
     => 2
    m.sum_of_ranges(:hour => "2012-04-13-17", :what => "load_time", :group => :session_id)
     => 286
    m.average_range(:hour => "2012-04-13-17", :what => "load_time", :group => :session_id)
     => 143
    m.maximum_range(:hour => "2012-04-13-17", :what => "load_time", :group => :session_id)
     => 158
    m.minimum_range(:hour => "2012-04-13-17", :what => "load_time", :group => :session_id)
     => 128
    m.stddev_of_ranges(:hour => "2012-04-13-17", :what => "load_time", :group => :session_id)
     => 15.0


    # Supposing there were instead millions of counter and measure operations,
    # metricstore may reach its list_threshold. Some queries will fail.

    m.list(:hour => "2012-04-13-17", :what => "load_time", :list => :page)
     => ['/welcome/', '/projects/']

    m.list(:hour => "2012-04-13-17", :what => "load_time", :list => :session_id)
    Metricstore::DataLossError: Too many session_id for "2012-04-13-17", "load_time".

    m.estimated_list_size(:hour => "2012-04-13-17", :what => "load_time", :list => :session_id)
     => 3560831

    m.close

## EventMachine

The Metricstore client's write methods (counter, measure) are designed to run
within an [EventMachine](http://rubyeventmachine.com/) reactor. This allows
writes to be batched up together (only when there's a backlog), and to re-try
in the case of intermittent connection problems or other non-fatal errors. You
will want to design your app to leave the reactor running.

If it does not make sense to leave a reactor running in your app, you can
make your updates within a temporary reactor using the client's "run" method.
Be aware though, that the "run" method itself will block until the write backlog
is clear again.

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
