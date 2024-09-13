class cars:
    def colour(self):
        print("this is colour method")

    def milage(self):
        print("this is milage method")


class suzuki(cars):
    def kind(self):
        print("this is kind method")


class nexon(suzuki):
    def strength(self):
        print("this is strength method")


# Create an instance of nexon
n = nexon()

# Call methods using the instance
n.strength()
n.colour()
n.milage()
n.kind()