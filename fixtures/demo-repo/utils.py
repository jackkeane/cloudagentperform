def average(numbers):
    # TODO: handle empty list
    return sum(numbers) / len(numbers)

def clamp(value, low, high):
    return max(low, min(high, value))
