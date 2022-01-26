def insertion_sort(arr):
    for index in range(1,len(arr)):
        cur_val = arr[index]
        cur_pos = index

        while cur_pos >0 and arr[cur_pos-1]>cur_val:
            arr[cur_pos] = arr[cur_pos-1]
            cur_pos = cur_pos-1

        arr[cur_pos] = cur_val       

    return arr
unsort_list  = input("Enter the list/array needs to be sorted:")


print(insertion_sort(unsort_list))