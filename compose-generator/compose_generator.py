from jinja2 import Environment, FileSystemLoader

def ask_for_int_input(text):
    value = 0
    while value < 1:
        value = int(input(text))
    return value

def generate_compose():
    reducers1 = ask_for_int_input("How many reducers for ex1? ")
    reducers2 = ask_for_int_input("How many reducers for ex2? ")
    stopovers = ask_for_int_input("How many stopovers filter? ")
    processors = ask_for_int_input("How many data processors? ")
    distances = ask_for_int_input("How many distance filters? ")
    completers = ask_for_int_input("How many distance completers? ")
    ex4Savers = ask_for_int_input("How many journey savers for ex4? ")
    ex4Dispatchers = ask_for_int_input("How many dispatchers for ex4? ")
    healthCheckers = ask_for_int_input("How many healthCheckers? ")
    calculators = ask_for_int_input("How many Average Calculators? ")

    env = Environment(loader=FileSystemLoader("templates/"))

    template = env.get_template("docker-compose-template.yaml")

    filename = f"generated/docker-compose.yaml"
    with open(filename, mode="w", encoding="utf-8") as output:
        output.write(template.render(
            reducers1=reducers1,
            reducers2=reducers2,
            stopovers=stopovers,
            processors=processors,
            distances=distances,
            completers=completers,
            ex4Savers=ex4Savers,
            ex4Dispatchers=ex4Dispatchers,
            healthcheckers=healthCheckers,
            calculators=calculators
        )
    )
        
    print(f"Wrote docker compose to {filename}")

if __name__ == "__main__":
    generate_compose()