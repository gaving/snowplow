# Copyright (c) 2012-2018 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::    Ben Fradet (mailto:support@snowplowanalytics.com)
# Copyright:: Copyright (c) 2012-2018 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'aws-sdk-s3'
require 'contracts'
require 'pathname'
require 'uri'

module Snowplow
  module EmrEtlRunner
    module S3

      include Contracts

      # Check a location on S3 is empty.
      #
      # Parameters:
      # +client+:: S3 client
      # +location+:: S3 url of the folder to check for emptiness
      # +key_filter+:: filter to apply on the keys, filters folders and $folder$ files by default
      def empty?(client, location,
          key_filter = lambda { |k| !(k =~ /\/$/) and !(k =~ /\$folder\$$/) })
        bucket, prefix = parse_bucket_prefix(location)
        empty_implem(client, bucket, prefix, key_filter)
      end

      # Extract the bucket and prefix from an S3 url.
      #
      # Parameters:
      # +location+:: the S3 url to parse
      Contract String => [String, String]
      def parse_bucket_prefix(location)
        u = URI.parse(location)
        return u.host, u.path[1..-1]
      end

    private

      def empty_implem(client, bucket, prefix, key_filter, max_keys = 10, token = nil)
        options = {
          bucket: bucket,
          prefix: prefix,
          max_keys: max_keys,
        }
        options[:continuation_token] = token if !token.nil?
        response = client.list_objects_v2(options)
        filtered = response.contents.select { |c| key_filter[c.key] }
        if filtered.empty?
          if response.is_truncated
            empty_implem(client, bucket, prefix, key_filter, max_keys, response.next_continuation_token)
          else
            true
          end
        else
          false
        end
      end

    end
  end
end
