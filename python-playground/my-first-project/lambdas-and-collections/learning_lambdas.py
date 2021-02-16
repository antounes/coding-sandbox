def square(num):
    return num * num

square_lambda = lambda num: num * 1

assert square(4) == square_lambda(4)