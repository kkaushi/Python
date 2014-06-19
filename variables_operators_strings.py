#variables and math
cars = 100
space_in_a_car = 4.0
drivers = 30 
passengers = 90
cars_not_driven=cars - drivers
cars_driven = drivers
carpool_capacity = cars_driven * space_in_a_car
average_passengers_per_car = passengers/cars_driven

print("There are ",cars," cars available")
print("We need to put about ", average_passengers_per_car," in each car.")

#strings
my_name = 'kuldeep kaushik'
my_age = 27
my_education = 'masters'

print("Let's talk about %s." % my_name)
print("He is %d years old and is doing his %s."%(my_age,my_education));

two = 2
three = 3
print("%d plus %d is %d"%(two,two,two+three));


# %r is used to print string in quotes
x = "This is good"
print("I said: %r"%x);

#repeating a string 10 times THIS IS COOL!
print ("."*10);