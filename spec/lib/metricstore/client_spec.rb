require File.dirname(__FILE__) + '/../../spec_helper'
#require 'ruby-prof'

module Metricstore
  describe Client do
    context "supposing the current time is 17:05 UTC on April 13, 2012" do
      around(:each) do |spec|
        Timecop.travel(Time.utc(2012,4,13,17,5)) do
          @mock_key_value_client = MockKeyValueClient.new
          @client = Metricstore::Client.new(
            :kvstore => @mock_key_value_client,
            :sleep_interval => 0.001,
            :max_healthy_errors => 0,
            :max_unhandled_errors => 0,
            :max_retry_delay_in_seconds => 60.0
          )
          @client.max_ttl_of_dimension[:session_id] = 60
          @client.run do
            @n = 10
  #          profile_data = RubyProf.profile do
              @n.times do
                @client.counter(:what => "logins", :where => {:user => 'joe', :ip => '10.20.30.40'})
                @client.counter(:what => "logins", :where => {:user => 'bob', :ip => '10.20.30.40'})
                @client.counter(:what => "logins", :where => {:user => 'joe', :ip => '10.20.30.50'})
                @client.counter(:what => "logouts", :where => {:user => 'joe', :ip => '10.20.30.40'})

                @client.measure(:what => "load_time", :value => 340, :where => {:page => '/welcome/', :session_id => "h0zhmb1c-u1xfgw305e"})
                @client.measure(:what => "load_time", :value => 501, :where => {:page => '/welcome/', :session_id => "h0zhmb2q-643dotlcgd"})
                @client.measure(:what => "load_time", :value => 212, :where => {:page => '/welcome/', :session_id => "h0zhmb1c-u1xfgw305e"})
                @client.measure(:what => "load_time", :value => 343, :where => {:page => '/welcome/', :session_id => "h0zhmb2q-643dotlcgd"})
              end
  #          end

            @client.list_threshold = 10

            @sheep_count = @client.list_threshold + 1
            @sheep_count.times do
              @client.counter(:what => 'sheep', :where => {:id => rand(2**128).to_s(36)})
            end

            # result_path = Pathname.new(__FILE__).join('..','..','..','..','profile').expand_path
            # result_path.join('calltree.data').open('w') do |f|
            #   RubyProf::CallTreePrinter.new(profile_data).print(f)
            # end
            # result_path.join('graph.html').open('w') do |f|
            #   RubyProf::GraphHtmlPrinter.new(profile_data).print(f)
            # end
          end

          @client.run do
            spec.run
          end

          @mock_key_value_client = nil
          @client = nil
        end
      end

      it "should work like the README says it does" do
        @client.count(:hour => '2012-04-13-17', :what => "logins").should == 3 * @n
        @client.list(:hour => '2012-04-13-17', :what => "logins", :list => :user).should == ['joe', 'bob']
        @client.count(:hour => '2012-04-13-17', :what => "logins", :where => {:user => 'joe'}).should == 2 * @n
        @client.count(:hour => '2012-04-13-17', :what => "logins", :where => {:user => 'bob'}).should == 1 * @n
        @client.list(:hour => '2012-04-13-17', :what => "logins", :where => {:user => 'joe'}, :list => :ip).should == ['10.20.30.40', '10.20.30.50']
        @client.list(:hour => '2012-04-13-17', :what => "logins", :where => {:user => 'bob'}, :list => :ip).should == ['10.20.30.40']
        @client.count(:hour => '2012-04-13-17', :what => "logins", :where => {:user => 'joe', :ip => '10.20.30.40'}).should == 1 * @n

        @client.count(:hour => "2012-04-13-17", :what => "load_time").should == 4 * @n
        @client.sum(:hour => "2012-04-13-17", :what => "load_time").should == 1396 * @n
        @client.average(:hour => "2012-04-13-17", :what => "load_time").should == 349.0
        @client.maximum(:hour => "2012-04-13-17", :what => "load_time").should == 501
        @client.minimum(:hour => "2012-04-13-17", :what => "load_time").should == 212
        @client.stddev(:hour => "2012-04-13-17", :what => "load_time").should be_within(0.00000001).of 102.45730818248154
        @client.list(:hour => "2012-04-13-17", :what => "load_time", :list => :page).should == ['/welcome/']

        @client.count_of_groups(:hour => "2012-04-13-17", :what => "load_time", :group => :session_id).should == 2
        @client.sum_of_ranges(:hour => "2012-04-13-17", :what => "load_time", :group => :session_id).should == 286
        @client.average_range(:hour => "2012-04-13-17", :what => "load_time", :group => :session_id).should == 143
        @client.maximum_range(:hour => "2012-04-13-17", :what => "load_time", :group => :session_id).should == 158
        @client.minimum_range(:hour => "2012-04-13-17", :what => "load_time", :group => :session_id).should == 128
        @client.stddev_of_ranges(:hour => "2012-04-13-17", :what => "load_time", :group => :session_id).should == 15.0

        @client.estimated_list_size(:hour => "2012-04-13-17", :what => "load_time", :list => :session_id).should == 2

        expect { @client.list(:hour => "2012-04-13-17", :what => "sheep", :list => :id) }.to raise_exception(Metricstore::DataLossError)

        @client.estimated_list_size(:hour => "2012-04-13-17", :what => "sheep", :list => :id).should be_within((1.05 * Client::CARDINALITY_ESTIMATOR_ERROR_RATE * @sheep_count).ceil).of (@sheep_count)
      end
    end
  end
end
