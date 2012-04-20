require File.dirname(__FILE__) + '/../../spec_helper'
require 'em-synchrony'

module Metricstore
  describe Client do
    context "supposing the current time is 17:05 UTC on April 13, 2012" do
      around(:each) do |spec|
        Timecop.travel(Time.utc(2012,4,13,17,5))
        @mock_key_value_client = MockKeyValueClient.new
        @client = Metricstore::Client.new(
          :kvstore => @mock_key_value_client,
          :sleep_interval => 0.001,
          :max_healthy_errors => 0,
          :max_unhandled_errors => 0,
          :max_retry_delay_in_seconds => 60.0
        )
        EM.synchrony do
          @client.open

          @n = 100
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


          EM.add_timer(1.2) do

            spec.run

            EM.next_tick do
              @mock_key_value_client = nil
              @client.close
              @client = nil
              EM.stop
              Timecop.return
            end
          end
        end
      end

      it "should work like the README says it does" do
        @client.count(:when => '2012-04-13-17', :what => "logins").should == 3 * @n
        @client.list(:when => '2012-04-13-17', :what => "logins", :list => :user).should == ['joe', 'bob']
        @client.count(:when => '2012-04-13-17', :what => "logins", :where => {:user => 'joe'}).should == 2 * @n
        @client.count(:when => '2012-04-13-17', :what => "logins", :where => {:user => 'bob'}).should == 1 * @n
        @client.list(:when => '2012-04-13-17', :what => "logins", :where => {:user => 'joe'}, :list => :ip).should == ['10.20.30.40', '10.20.30.50']
        @client.list(:when => '2012-04-13-17', :what => "logins", :where => {:user => 'bob'}, :list => :ip).should == ['10.20.30.40']
        @client.count(:when => '2012-04-13-17', :what => "logins", :where => {:user => 'joe', :ip => '10.20.30.40'}).should == 1 * @n

        @client.count(:when => "2012-04-13-17", :what => "load_time").should == 4 * @n
        @client.sum(:when => "2012-04-13-17", :what => "load_time").should == 1396 * @n
        @client.average(:when => "2012-04-13-17", :what => "load_time").should == 349.0
        @client.maximum(:when => "2012-04-13-17", :what => "load_time").should == 501
        @client.minimum(:when => "2012-04-13-17", :what => "load_time").should == 212
        @client.stddev(:when => "2012-04-13-17", :what => "load_time").should == 102.45730818248154
        @client.list(:when => "2012-04-13-17", :what => "load_time", :list => :page).should == ['/welcome/']

        @client.count_of_groups(:when => "2012-04-13-17", :what => "load_time", :group => :session_id).should == 2
        @client.sum_of_ranges(:when => "2012-04-13-17", :what => "load_time", :group => :session_id).should == 286
        @client.average_range(:when => "2012-04-13-17", :what => "load_time", :group => :session_id).should == 143
        @client.maximum_range(:when => "2012-04-13-17", :what => "load_time", :group => :session_id).should == 158
        @client.minimum_range(:when => "2012-04-13-17", :what => "load_time", :group => :session_id).should == 128
        @client.stddev_of_ranges(:when => "2012-04-13-17", :what => "load_time", :group => :session_id).should == 15.0

        @client.estimated_list_size(:when => "2012-04-13-17", :what => "load_time", :list => :session_id).should == 2
      end
    end
  end
end
