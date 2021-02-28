# You are given an array prices for which the ith element is the price of a given stock on day i.
# Find the maximum profit you can achieve.
# You may complete as many transactions as you like
# (i.e., buy one and sell one share of the stock multiple times).

def maxProfit(prices):
    s = 0
    for i in range(0, len(prices)-1):
        if prices[i+1] > prices[i]:
            s += (prices[i+1] - prices[i])
    return s


if __name__ == "__main__":
    list_prices = [[7,1,5,3,6,4], [1,2,3,4,5], [7,6,4,3,1]]
    for prices in list_prices:
        print("Prices:", prices, "MaxProfit(prices):", maxProfit(prices))
