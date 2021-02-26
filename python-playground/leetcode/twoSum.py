# Given an array of integers nums and an integer target, return indices of
# the two numbers such that they add up to target.

def twoSum(nums, target):
    for i in range(0, len(nums)):
        for j in range(i+1, len(nums)):
            if nums[i] + nums[j] == target:
                return [i, j]
        j += 1
    i += 1


if __name__ == "__main__":
    nums = [[2,7,11,15], [3,2,4], [3,3]]
    targets = [9, 6, 6]
    for num, tar in zip(nums, targets):
        print("nums:", num, "target:", tar, twoSum(num, tar))