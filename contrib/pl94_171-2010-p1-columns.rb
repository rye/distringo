# This script prints out the column names of the P1 dataset in order.

categories = [
	'White',
	'Black or African American',
	'American Indian and Alaska National',
	'Asian',
	'Native Hawaiian and Other Pacific Islander',
	'Some Other Race',
]

puts "Total"
puts "Population of one race"

categories.each do |category|
	puts "#{category} alone"
end

puts "Two or More Races"

puts "Population of two races"

categories.combination(2).each do |category_group|
	puts category_group.join("; ")
end

puts "Population of three races"

categories.combination(3).each do |category_group|
	puts category_group.join("; ")
end

puts "Population of four races"

categories.combination(4).each do |category_group|
	puts category_group.join("; ")
end

puts "Population of five races"

categories.combination(5).each do |category_group|
	puts category_group.join("; ")
end

puts "Population of six races"

categories.combination(6).each do |category_group|
	puts category_group.join("; ")
end
