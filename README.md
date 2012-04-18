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
    m.ttl_of_minutes = 86_400 # 24 hours, default
    m.ttl_of_group_members = 7200 # 2 hours, default
    m.list_threshold = 1000 # default

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

    m.count(:when => "2012-04-13-17", :what => "logins")
     => 3
    m.list(:when => "2012-04-13-17", :what => "logins", :list => :user)
     => ['joe', 'bob']
    m.count(:when => "2012-04-13-17", :what => "logins", :where => {:user => 'joe'})
     => 2
    m.count(:when => "2012-04-13-17", :what => "logins", :where => {:user => 'bob'})
     => 1
    m.list(:when => "2012-04-13-17", :what => "logins", :where => {:user => 'joe'}, :list => :ip)
     => ['10.20.30.40', '10.20.30.50']
    m.list(:when => "2012-04-13-17", :what => "logins", :where => {:user => 'bob'}, :list => :ip)
     => ['10.20.30.40']
    m.count(:when => "2012-04-13-17", :what => "logins", :where => {:user => 'joe', :ip => '10.20.30.40'})
     => 1

    m.count(:when => "2012-04-13-17", :what => "load_time")
     => 4
    m.sum(:when => "2012-04-13-17", :what => "load_time")
     => 1396
    m.average(:when => "2012-04-13-17", :what => "load_time")
     => 349.0
    m.maximum(:when => "2012-04-13-17", :what => "load_time")
     => 501
    m.minimum(:when => "2012-04-13-17", :what => "load_time")
     => 212
    m.stddev(:when => "2012-04-13-17", :what => "load_time")
     => 102.45730818248154
    m.list(:when => "2012-04-13-17", :what => "load_time", :list => :page)
     => ['/welcome/']

    # We can do queries related to groups as well, with some limitations.
    # We only guarantee the accuracy of the result if all related data was
    # loaded from start-to-finish within :ttl_of_group_members seconds.
    # Note: a range is the difference between the minimum and maximum metric,
    # for an individual group.
    m.count_of_groups(:when => "2012-04-13-17", :what => "load_time", :group => :session_id)
     => 2
    m.sum_of_ranges(:when => "2012-04-13-17", :what => "load_time", :group => :session_id)
     => 286
    m.average_range(:when => "2012-04-13-17", :what => "load_time", :group => :session_id)
     => 143
    m.maximum_range(:when => "2012-04-13-17", :what => "load_time", :group => :session_id)
     => 158
    m.minimum_range(:when => "2012-04-13-17", :what => "load_time", :group => :session_id)
     => 128
    m.stddev_of_ranges(:when => "2012-04-13-17", :what => "load_time", :group => :session_id)
     => 15.0


    # Supposing there were instead millions of counter and measure operations,
    # metricstore may reach its list_threshold. Some queries will fail.

    m.list(:when => "2012-04-13-17", :what => "load_time", :list => :page)
     => ['/welcome/', '/projects/']

    m.list(:when => "2012-04-13-17", :what => "load_time", :list => :session_id)
    metricstore::DataLossError: Too many session_id for "2012-04-13-17", "load_time".

    m.estimated_list_size(:when => "2012-04-13-17", :what => "load_time", :list => :session_id)
     => 3560831


## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Added some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
