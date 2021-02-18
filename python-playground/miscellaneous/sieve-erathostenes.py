# Python program to print all primes smaller than  or equal to
# a given n using Sieve of Eratosthenes

def sieve_of_eratosthenes(n):
    # Create a boolean array prime and initialise all its entries as True
    # A value in prime will finally be False if i is not a prime, otherwise True

    prime = [True]*(n + 1)
    p = 2
    while p ** 2 <= n:
        # If prime[p] remains unchanged, then it's a prime number
        if prime[p]:
            for i in range(p ** 2, n + 1, p):
                prime[i] = False
        p += 1
    prime[0] = False
    prime[1] = False

    # Print all prime numbers
    for p in range(n + 1):
        if prime[p]:
            print(p)


if __name__ == "__main__":
    n = int(input("Look for prime numbers up to: "))
    sieve_of_eratosthenes(n)
