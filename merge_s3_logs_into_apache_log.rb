#
# Connects to S3, downloads your logs and merges them into your local
# Apache logs.  Then you can run Webalizer or Awstats on your local
# Apache log and S3 hits will be included.
#
# May 7, 2009
# Kevin Bombino
# kevin@bombino.org
#
# Released into the public domain.
#

require 'aws/s3'

ACCESS_KEY = "CHANGEME"
SECRET_ACCESS_KEY = "CHANGEME"
BUCKET = "logs.mysite.com"
LOCAL_APACHE_LOG = "/var/www/vhosts/mysite.com/statistics/logs/access_log"

class SyncLogs
  
  def decode_s3_line(line)
    raw = CSV.parse_line(line, " ")
    return nil unless raw[7] == "REST.GET.OBJECT"
    {
      :ip => raw[4], 
      :time => "#{raw[2]} #{raw[3]}", 
      :request => raw[9], 
      :status => raw[10], 
      :size => raw[12],
      :referrer => raw[16],
      :useragent => raw[17],
      :request_id => raw[6]        
    }
  end
  
  def decode_s3_log(logfile_content)
    logfile_content.collect { |line| 
      decode_s3_line(line) 
    }.reject { |log_entry| 
      log_entry.nil? 
    }
  end
  
  def encode_apache_line(log_entry)
    "#{log_entry[:ip]} - - #{log_entry[:time]} \"#{log_entry[:request]}\" #{log_entry[:status]} #{log_entry[:size]} \"#{log_entry[:referrer]}\" \"#{log_entry[:useragent]}\""
  end
  
  def encode_apache_log(log_entries)
    log_entries.collect { |log_entry|
      encode_apache_line(log_entry)
    }.join("\n")
  end
  
  def establish_connection!
    AWS::S3::Base.establish_connection!(
      :access_key_id     => ACCESS_KEY,
      :secret_access_key => SECRET_ACCESS_KEY
    )
  end

  def s3_log_to_apache_log(log)
    encode_apache_log(decode_s3_log(logfile_content))
  end

  def run!
    establish_connection!
    
    # grab s3 logs, convert them to apache format
    s3_logs = AWS::S3::Bucket.find(BUCKET).objects[0, 480]
    apache_logfile_data = s3_logs.collect { |log| 
      s3_log_to_apache_log(log.value) 
    }.join("\n")
    
    # delete logs from server so we don't include them again
    s3_logs.each { |log| log.delete }
    
    # stick s3 data into apache log, sort the log file so Webalizer works
    File.open(LOCAL_APACHE_LOG, 'a') {|f| f.write(apache_logfile_data) }
    `cp #{file} #{file}.2`
    `sort -t ' ' -k 4.9,4.12n -k 4.5,4.7M -k 4.2,4.3n -k 4.14,4.15n -k 4.17,4.18n -k 4.20,4.21n #{LOCAL_APACHE_LOG}.2 > #{LOCAL_APACHE_LOG}`
    `rm -rf #{LOCAL_APACHE_LOG}.2`
  end

end

if __FILE__ == $0
  SyncLogs.new.run!
end