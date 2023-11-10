from jinja2 import Environment, FileSystemLoader

def generate_compose():
    reducers1 = 0
    reducers2 = 0
    stopovers = 0
    processors = 0
    distances = 0
    completers = 0
    ex4Savers = 0
    ex4Dispatchers = 0
    healthCheckers = 0

    while reducers1 < 1:
        reducers1 = int(input("How many reducers for ex1? "))

    while reducers2 < 1:
        reducers2 = int(input("How many reducers for ex2? "))

    while stopovers < 1:
        stopovers = int(input("How many stopovers filter? "))

    while processors < 1:
        processors = int(input("How many data processors? "))

    while distances < 1:
        distances = int(input("How many distance filters? "))

    while completers < 1:
        completers = int(input("How many distance completers? "))

    while ex4Savers < 1:
        ex4Savers = int(input("How many journey savers for ex4? "))

    while ex4Dispatchers < 1:
        ex4Dispatchers = int(input("How many dispatchers for ex4? "))

    while healthCheckers < 1:
        healthCheckers = int(input("How many healthCheckers? "))

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
            healthcheckers=healthCheckers
        )
    )
        
    print(f"Wrote docker compose to {filename}")

if __name__ == "__main__":
    generate_compose()