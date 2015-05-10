require 'algoliasearch'

class IndexProcessor

  @@country = {}
  @@algolia_index = nil  
  @@count_cities = 0
  
  #get country names and iso codes
  def self.country_iso_list
    IO.foreach( COUNTRY_ISO_FILE ) { |line| 
      splits = line.split("\t")
      iso, iso3, iso_numeric, fips, country_name, capital, area, population, continent, tld, currency_code, currency_name, phone = splits
      @@country["#{iso}"] = country_name
    }
    return @@country
  end

  def self.parse_cities 
    tmp = []

    IO.foreach(DATA_FILE) { |line|      
      
      city = IndexProcessor.parse_line(line, FORMAT)
      unless city.nil?
        tmp << city
        @@count_cities  += 1
        # puts to console city data to see what's going on
        puts "#{@@count_cities} ** #{tmp.last}"
      end

      # index each 10K cities
      if tmp.size == BATCH_SIZE
        IndexProcessor.algolia_index(tmp,BATCH_SIZE)
        tmp.clear
      end
    }

    # indexing last records, tmp might have less than BATCH_SIZE records
    if tmp.size > 0
      IndexProcessor.algolia_index(tmp,BATCH_SIZE)
      tmp.clear
    end

    return @@count_cities
  end

  def self.parse_line(line, format)
    city = nil
    case format
      when 'csv'
        splits = line.split(",")
        id, country_code, region, city_name, postalCode, latitude, longitude, metro_code, area_code = splits
        puts country_code 
        city_name     = eval(city_name) unless city_name.nil?
        country_code  = eval(country_code) unless country_code.nil?
        
        city = {
                :objectID => id.to_i,
                :name => city_name,
                :alternatenames => "",
                :country_code => country_code,
                :country_name => @@country["#{country_code}"],
                :_geoloc => {
                  :lat => latitude.to_f,
                  :lng => longitude.to_f
                }
              }
      when 'txt'
        splits = line.split("\t")
        geonameid, name, asciiname, alternatenames, latitude, longitude, feature_class , feature_code , country_code , cc2, admin1_code, admin2_code, admin3_code, admin4_code, population, elevation, dem, timezone, modification_date = splits
      
        if feature_code == 'PPL'
          city = {
              :objectID => geonameid.to_i,
              :name => name,
              :alternatenames => alternatenames,
              :country_code => country_code,
              :country_name => @@country["#{country_code}"],
              :_geoloc => {
                :lat => latitude.to_f,
                :lng => longitude.to_f
              }
            }
        end 
    end
    return city
  end

  def self.algolia_init(index_name)
    Algolia.init  :application_id => ENV['ALGOLIA_APPLICATION_ID'],
                  :api_key => ENV['ALGOLIA_API_KEY']
    @@algolia_index = Algolia::Index.new(index_name)   
    @@algolia_index.clear_index    
    @@algolia_index.set_settings({
     :attributesToIndex => ["name","alternatenames","country_name"],
     :ranking => ["exact","geo"],
     :minWordSizefor1Typo => 6,
     :minWordSizefor2Typos => 12,
     :distinct => true,
     :attributeForDistinct => "country_name"
     })
  end

  def self.algolia_index(records, batch_size)
    puts "### Request sent to index #{records.size} cities... ###"
    records.each_slice(batch_size) do |batch|
      @@algolia_index.add_objects(batch)
    end 
  end

end


COUNTRY_ISO_FILE  = 'countryInfo.txt'
BATCH_SIZE        = 10000

puts "Please chose 1 for CSV or 2 for TXT:"
format = gets.chomp
case format 
  when "1"
    FORMAT    = 'csv'
    DATA_FILE = 'GeoLiteCity-Location.csv'
    puts "### Parsing CSV file..."
  when "2"
    FORMAT    = 'txt'
    DATA_FILE = 'allCountries.txt'
    puts "### Parsing TXT file..."
  else
end

puts "Please enter algolia index name:"
index_name = gets.chomp

IndexProcessor.algolia_init(index_name)
IndexProcessor.country_iso_list
count_cities =  IndexProcessor.parse_cities 

puts "Total cities indexed: #{count_cities}"




