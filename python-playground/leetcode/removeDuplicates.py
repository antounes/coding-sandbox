# Given a sorted array, remove the duplicates in-place such that each element appears
# only once

def removeDuplicates(nums):
    for i in range(0, len(nums)):
        for j in range(i+1, len(nums)):
            if nums[j] != nums[i]:
                i += 1
            nums[j], nums[i] = nums[i], nums[j]
    return i + 1

if __name__ == "__main__":
    nums = [0,0,1,1,1,2,2,3,3,4]
    print("nums", nums, "Remove duplicates", removeDuplicates(nums))
    print("")
    nums = [1, 1, 2]
    print("nums", nums, "Remove duplicates", removeDuplicates(nums))