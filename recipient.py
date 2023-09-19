import csv

class Person:
    def __init__(self, name, email, phone):
        self.name = name
        self.email = email
        self.phone = phone

class Role:
    def __init__(self, name):
        self.name = name
        self.members = []

    def add_member(self, person):
        self.members.append(person)

def load_data_from_csv():
    roles = {}
    people = {}

    # Load roles from role.csv
    with open('role.csv', mode='r') as file:
        reader = csv.reader(file)
        for row in reader:
            role_name = row[0]
            roles[role_name] = Role(role_name)

    # Load people from people.csv
    with open('people.csv', mode='r') as file:
        reader = csv.reader(file)
        for row in reader:
            name, email, phone = row
            people[name] = Person(name, email, phone)

    # Assign people to roles from role-people.csv
    with open('role-people.csv', mode='r') as file:
        reader = csv.reader(file)
        for row in reader:
            role_name, person_name = row
            role = roles.get(role_name)
            person = people.get(person_name)
            if role and person:
                role.add_member(person)

    return roles, people


roles, people = load_data_from_csv()
print ([person.email for person in roles['Admin'].members])
#
# for role_name, role in roles.items():
#     print(f"Role: {role_name}")
#     for person in role.members:
#         print(f"  Member: {person.name}, Email: {person.email}, Phone: {person.phone}")

