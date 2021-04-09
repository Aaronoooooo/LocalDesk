package com.aaron.algorithm.leetcode;

/**
 * @author Aaron
 * @version V1.0
 * @Package com.aaron.leetcode
 * @date 2021/3/30 11:06
 *  1.无序数组Two Sum
 *  https://leetcode.com/problems/two-sum/description/
 */


import java.util.Arrays;
import java.util.HashMap;

/**
 * 设置一个 map 容器 record 用来记录元素的值与索引，然后遍历数组 nums。
 * 每次遍历时使用临时变量 complement 用来保存目标值与当前值的差值
 * 在此次遍历中查找 record ，查看是否有与 complement 一致的值，如果查找成功则返回查找值的索引值与当前变量的值 i
 * 如果未找到，则在 record 保存该元素与索引值 i
 * nums[1,2,3,4,5]  target=7
 * return nums.index[1,4, 2,3]
 */
public class LC_1_TwoSum {
    public static void main(String[] args) {
        //int[] ints = new int[]{1, 3, 5, 7, 9, 2, 4, 6, 8};
        int[] ints = {1, 3, 5, 7, 9, 2, 4, 6, 8};
        int[] twoSumIndex = twoSum1(ints, 10);
        System.out.println(Arrays.toString(twoSumIndex));
    }

    // 线性查找
    // 时间复杂度：O(n^2)
    // 空间复杂度：O(1)
    public static int[] twoSum1(int[] nums, int target) {
        if (nums == null || nums.length == 0) return new int[0];
        for (int i = 0; i < nums.length; i++) {
            int x = nums[i];
            //线性查找(从第二个元素开始查找)
            for (int j = i + 1; j < nums.length; j++) {
                if (nums[j] == target - x) {
                    return new int[]{i, j};
                }
            }
        }
        return new int[0];
    }

    // 哈希查找
    // 时间复杂度：O(2n)
    // 空间复杂度：O(n)
    public static int[] twoSum2(int[] nums, int target) {
        if (nums == null || nums.length == 0) return new int[0];

        //数据预处理,保存下数组中的每个元素及对应索引
        HashMap<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < nums.length; i++) {
            map.put(nums[i], i);
        }
        for (int i = 0; i < nums.length; i++) {
            int x = nums[i];
            if (map.containsKey(target - x)) {
                Integer index = map.get(target - x);
                // i 和 index 不是同一个元素，同一个元素不能使用两次
                if (i != index) return new int[]{i, index};
            }
        }
        return new int[0];
    }

    // 哈希查找优化
    // 时间复杂度：O(n)
    // 空间复杂度：O(n)
    public static int[] twoSum3(int[] nums, int target) {
        if (nums == null || nums.length == 0) return new int[0];

        //数据预处理,保存下数组中的每个元素及对应索引
        HashMap<Integer, Integer> map = new HashMap<>();
        //for (int i = 0; i < nums.length; i++) {
        //    map.put(nums[i], i);
        //}
        for (int i = 0; i < nums.length; i++) {
            int x = nums[i];
            if (map.containsKey(target - x)) {
                Integer index = map.get(target - x);
                // i 和 index 不是同一个元素，同一个元素不能使用两次
                return new int[]{i, index};
            }
            map.put(nums[i], i);
        }
        return new int[0];
    }
}