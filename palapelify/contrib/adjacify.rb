data = open('pairs.csv', 'rb') do |io|
	io.read
end.split("\n")

adjacencies = {}

data.each do |line|
	spl = line.split(',')
	a_id = spl.first
	b_id = spl.last

	adjacencies[a_id] ||= []
	adjacencies[a_id] << b_id

	adjacencies[b_id] ||= []
	adjacencies[b_id] << a_id
end

p adjacencies.map{|a_id, bs| [a_id, bs.count]}.to_h

p adjacencies.count
