require 'json'
require 'set'

data = 'data/tl_2010_18157_tabblock10.min.geojson'

data = open(data, 'rb') do |io|
	JSON.parse(io.read)
end

n_features = data['features'].count
n_pairs = (1..n_features).reduce(1, :*) / (2 * (1..(n_features - 2)).reduce(1, :*))

t0 = Time.now
examined_so_far = 0

out = open('pairs2.csv', 'wb')

data['features'].combination(2).each do |combo|
	a = combo.first
	b = combo.last

	a_name = a['properties']['NAME10']
	b_name = b['properties']['NAME10']

	a_id = a['properties']['GEOID10']
	b_id = b['properties']['GEOID10']

	a_coords = a['geometry']['coordinates'].first.to_set
	b_coords = b['geometry']['coordinates'].first.to_set

	shared = a_coords & b_coords

	examined_so_far += 1

	if examined_so_far % 10000 == 0
		t = Time.now
		pct_done = examined_so_far.to_f / n_pairs
		rate = examined_so_far.to_f / (t - t0)
		puts "#{(pct_done * 100.0).round(3)}% done in #{(t - t0).round(3)}s... Rate = #{rate.round(3)} cases/s; ETA: #{((n_pairs - examined_so_far) / rate).round(3)}s"
	end

	if !shared.empty?
		if shared == a_coords || shared == b_coords
			puts "#{a_name} === #{b_name}"
		elsif shared.size == 1
			puts "#{a_name} >.< #{b_name}"
		elsif 1 < shared.size && (shared.size < a_coords.size) && (shared.size < b_coords.size)
			puts "#{a_name} >-< #{b_name}"
		else
			puts "#{a_name} >?< #{b_name}"
		end

		out.write("#{a_id},#{b_id},#{shared.size}\n")
	end
end
