# Given an array, rotate the array to the right by k steps, k >= 0

def rotate(nums, k):
    k = k % len(nums)
    nums[:] = nums[-k:] + nums[:-k]

def rotate(nums, k):
    i = 0
    while i < k:
        nums = [nums[-1]] + nums[:-1]
        i += 1

    print(nums)

if __name__ == "__main__":
    #nums = [1, 2, 3, 4, 5, 6, 7]
    #k = 3
    nums = [-1, -100, 3, 99]
    k = 2
    rotate(nums, k)
