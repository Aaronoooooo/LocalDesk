package com.aaron.algorithm.search;

import java.util.ArrayList;
import java.util.List;

//ע�⣺ʹ�ö��ֲ��ҵ�ǰ���� �������������.
public class BinarySearch {

    public static void main(String[] args) {
        int arr[] = {1, 3, 5, 100, 1000, 1000, 1000, 1045};
        //int arr[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20};

        //int resIndex = binarySearch(arr, 0, arr.length - 1, 1000);
        //System.out.println("resIndex=" + resIndex);

        List<Integer> resIndexList = binarySearch2(arr, 0, arr.length - 1, 1000);
        System.out.println("resIndexList=" + resIndexList);
    }

    // ���ֲ����㷨

    /**
     * @param arr     ����
     * @param left    ��ߵ�����
     * @param right   �ұߵ�����
     * @param findVal Ҫ���ҵ�ֵ
     * @return ����ҵ��ͷ����±�, ���û���ҵ�, �ͷ��� -1
     */

    public static int binarySearch(int[] arr, int left, int right, int findVal) {

        // �� left > right ʱ,˵���ݹ���������,����û���ҵ�
        int mid = (left + right) / 2;
        int midVal = arr[mid];
        if (left > right) {
            return -1;
        }
        if (findVal > midVal) {// �� �ҵݹ�
            return binarySearch(arr, mid + 1, right, findVal);
        } else if (findVal < midVal) { // ����ݹ�
            return binarySearch(arr, left, mid - 1, findVal);
        } else {
            return mid;
        }
    }

    /*
     * ��һ������������,�ж����ͬ����ֵʱ,��ν����е���ֵ�����ҵ�
     * ˼·����
     * 1. ���ҵ�mid ����ֵ,��Ҫ���Ϸ���
     * 2. ��mid ����ֵ�����ɨ��,���������� 1000, ��Ԫ�ص��±�,���뵽����ArrayList
     * 3. ��mid ����ֵ���ұ�ɨ��,���������� 1000, ��Ԫ�ص��±�,���뵽����ArrayList
     * 4. ��Arraylist����
     */

    public static List<Integer> binarySearch2(int[] arr, int left, int right, int findVal) {

        // �� left > right ʱ,˵���ݹ���������,����û���ҵ�
        if (left > right) {
            return new ArrayList<Integer>();
        }

        int mid = (left + right) / 2;
        int midVal = arr[mid];

        if (findVal > midVal) {
            return binarySearch2(arr, mid + 1, right, findVal);
        } else if (findVal < midVal) {
            return binarySearch2(arr, left, mid - 1, findVal);
        } else {
            ArrayList<Integer> arrayListIndex = new ArrayList<>();
            arrayListIndex.add(mid);

            //��mid��߿�ʼɨ��
            int tmp = mid - 1;
            while (true) {
                if (arr[tmp] != findVal || tmp < 0) {
                    break;
                }
                arrayListIndex.add(tmp);
                tmp -= 1;
            }

            //��mid�ұ߿�ʼɨ��
            tmp = mid + 1;
            while (true) {
                if (arr[tmp] != findVal || tmp > arr.length) {
                    break;
                }
                arrayListIndex.add(tmp);
                tmp += 1;
            }
            return arrayListIndex;
        }
    }
}
