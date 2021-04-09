package com.aaron.algorithm.leetcode;

import java.util.Arrays;

/**
 * @author Aaron
 * @version V1.0
 * @Package com.aaron.algorithm.leetcode
 * @date 2021/3/31 19:52
 * leetcode:https://leetcode-cn.com/problems/two-sum-ii-input-array-is-sorted/
 * 给定一个已按照 升序排列  的整数数组 numbers,请你从数组中找出两个数满足相加之和等于目标数 target,数组下标 从 1 开始计数
 */


public class LC_167_TwoSum {
    public static void main(String[] args) {
        int[] ints = {1, 2, 3, 4, 5, 6, 7, 8, 9};
        int[] twoSumIndex = twoSum(ints, 15);
        System.out.println(Arrays.toString(twoSumIndex));
    }

    /*二分查找
    时间复杂度：O(nlogn)
    空间复杂度：O(1)*/
    public static int[] twoSum(int[] nums, int target) {
        if (nums == null || nums.length == 0) return new int[0];

        for (int i = 0; i < nums.length; i++) {
            int x = nums[i];
            // 1. 二分查找 - O(logn)
            // 第一个数为x,从第二个数开始查找i+1,及[i + 1, nums.length - 1] 区间二分查找 target - x
            int index = binarySearch(nums, i + 1, nums.length - 1, target - x);
            if (index != -1) {
                return new int[]{i + 1, index + 1};
            }
        }

        return new int[0];
    }

    /*双指针
    时间复杂度：O(n)
    空间复杂度：O(1)*/
    public static int[] twoSum2(int[] nums, int target) {
        int low = 0, high = nums.length - 1;
        for (int i = 0, j = nums.length - 1; i < j;) {
            int sum = nums[i] + nums[j];
            if (sum == target) return new int[]{i + 1, j + 1};
            else if (sum > target) j--;
            else i++;
        }
        return new int[0];
    }

    /*双指针
     时间复杂度：O(n)
     空间复杂度：O(1)*/
    public static int[] twoSum3(int[] nums, int target) {
        int low = 0, high = nums.length - 1;
        while (low < high) {
            int sum = nums[low] + nums[high];
            if (sum == target) {
                // 下标 从 1 开始计数
                return new int[]{low + 1, high + 1};
            } else if (sum < target) {
                ++low;
            } else {
                --high;
            }
            return new int[]{-1, -1};
        }
        return new int[0];
    }

    private static int binarySearch(int[] nums, int left, int right, int target) {
        while (left <= right) {
            int mid = left + (right - left) / 2;
            if (nums[mid] == target) return mid;
            if (nums[mid] > target)
                right = mid - 1;
            else
                left = mid + 1;
        }
        return -1;
    }
}


