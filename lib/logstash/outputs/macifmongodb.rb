# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require 'mongo'

# An macifmongodb output that does nothing.
class LogStash::Outputs::Macifmongodb < LogStash::Outputs::Base
  config_name "macifmongodb"

    # A MongoDB URI to connect to.
  # See http://docs.mongodb.org/manual/reference/connection-string/.
  config :uri, :validate => :string, :required => true

  # The database to use.
  config :database, :validate => :string, :required => true

  # The collection to use. This value can use `%{foo}` values to dynamically
  # select a collection based on data in the event.
  config :collection, :validate => :string, :required => true

  # If true, store the @timestamp field in MongoDB as an ISODate type instead
  # of an ISO8601 string.  For more information about this, see
  # http://www.mongodb.org/display/DOCS/Dates.
  config :isodate, :validate => :boolean, :default => false

  # The number of seconds to wait after failure before retrying.
  config :retry_delay, :validate => :number, :default => 3, :required => false

  # If true, an "_id" field will be added to the document before insertion.
  # The "_id" field will use the timestamp of the event and overwrite an existing
  # "_id" field in the event.
  config :generateId, :validate => :boolean, :default => false


  # Bulk insert flag, set to true to allow bulk insertion, else it will insert events one by one.
  config :bulk, :validate => :boolean, :default => false
  # Bulk interval, Used to insert events periodically if the "bulk" flag is activated.
  config :bulk_interval, :validate => :number, :default => 2
  # Bulk events number, if the number of events to insert into a collection raise that limit, it will be bulk inserted
  # whatever the bulk interval value (mongodb hard limit is 1000).
  config :bulk_size, :validate => :number, :default => 900, :maximum => 999, :min => 2

  # champs constituant la clé : liste des champs permettant d'identifier la clé. Si cette liste n'est pas vide le plugin effectue un db.update et précise alors cette clé
  # Attention dans ce mode le bulk est inopérant
  config :key_fields, :validate => :array, :default => []

  # Mutex used to synchronize access to 'documents'
  @@mutex = Mutex.new

  public
  def register
	#    Mongo::Logger.logger = @logger
	@logger.debug("uri="+@uri.to_s)
	@logger.debug("database="+@database.to_s)
	@logger.debug("debug database="+@database.to_s)
	@uri_database=@uri+"/"+@database
    client = Mongo::Client.new(@uri_database)
	@conn_collection = client[@collection]

    if @bulk_size > 1000
      raise LogStash::ConfigurationError, "Bulk size must be lower than '1000', currently '#{@bulk_size}'"
    end
    @documents = {}
    Thread.new do
      loop do
        sleep @bulk_interval
        @@mutex.synchronize do
          @documents.each do |collection, values|
            if values.length > 0
              @db[collection].insert_many(values)
              @documents.delete(collection)
            end
          end
        end
      end
    end

  end # def register

  public
  def receive(event)
  
  
    begin
      # Our timestamp object now has a to_bson method, using it here
      # {}.merge(other) so we don't taint the event hash innards
	  @logger.warn("event="+event.to_s)
      document = {}.merge(event.to_hash)
      if !@isodate
        # not using timestamp.to_bson
        document["@timestamp"] = event.timestamp.to_json
      end
      if @generateId
        document["_id"] = BSON::ObjectId.new(nil, event.timestamp)
      end
	  
  	  @logger.trace("document="+document.to_s)
  	  filtre ={}
	  
      if @key_fields.length > 0
	           @key_fields.each_index do |i|
               field_name = @key_fields[i]
               field_value = document[field_name]
			   filtre=filtre.merge({field_name=>field_value})
           end
		   @logger.debug("filtre="+filtre.to_s)
		   
		   @logger.info("update_one : filtre="+filtre.to_s+"  document="+document.to_s)
		   @conn_collection.update_one(filtre,document,{upsert: true})

	  else
        if @bulk
           @@mutex.synchronize do
             collection = event.sprintf(@collection)
             if(!@documents[collection])
               @documents[collection] = []
             end
             @documents[collection].push(document)

             if(@documents[collection].length >= @bulk_size)
               @conn_collection.insert_many(@documents[collection])
               @documents.delete(collection)
             end
           end
        else
		    @logger.warn("db="+@db.to_s)
		    @logger.warn("collection="+@collection.to_s)
		    @logger.warn("document="+document.to_s)
            @conn_collection.insert_one(document)
        end

	  end




    rescue => e
      @logger.warn("Failed to send event to MongoDB", :event => event, :exception => e,
                   :backtrace => e.backtrace)
      if e.message =~ /^E11000/
          # On a duplicate key error, skip the insert.
          # We could check if the duplicate key err is the _id key
          # and generate a new primary key.
          # If the duplicate key error is on another field, we have no way
          # to fix the issue.
      else
        sleep @retry_delay
        retry
      end
    end
  
  
  
   # client = Mongo::Client.new('mongodb://127.0.0.1:27017/local')
	#collection = client[:test_logstash]

	#doc = { nom: 'Steve', hobbies: [ 'hiking', 'tennis', 'fly fishing' ] }

	#result = @conn_collection.insert_one(doc)
	
  end # def event
end # class LogStash::Outputs::Macifmongodb
