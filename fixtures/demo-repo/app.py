"""Tiny demo app used by the golden scan."""

def handle_request(payload):
    # TODO: validate input
    name = payload.get("name", "world")
    return greet(name)

def greet(name):
    # TODO: add logging
    return f"hello, {name}"

if __name__ == "__main__":
    print(handle_request({"name": "cap"}))
