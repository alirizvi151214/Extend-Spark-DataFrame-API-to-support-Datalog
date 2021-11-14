from pyspark.sql import SparkSession
import datalog_ds
from pyspark.sql.functions import col


def get_atom_df_mapping(df_list, each_atom_args):
    map_dict = dict()
    cons_filter_dict = dict()
    for each_index in range(0, len(df_list)):
        if type(each_atom_args[each_index]) == datalog_ds.DLVariable:
            map_dict[each_atom_args[each_index]] = str(df_list[each_index])
        else:
            cons_filter_dict[df_list[each_index]] = str(each_atom_args[each_index])

    return map_dict, cons_filter_dict


def filter_atom_df(df, cons_dict):
    for k, v in cons_dict.items():
        df = df.filter((col(k) == v))
    return df


def get_datalog_result(mapping_dict, datalog_rule_body):
    all_col_atom_dict = dict()
    transformed_df_list = []
    for each_atom in datalog_rule_body:
        predicate = each_atom.predicate_name
        if predicate in mapping_dict:
            atom_df = mapping_dict[predicate]

            df_col_list = atom_df.columns
            each_atom_args = each_atom.args_list

            col_atom_dict, cons_filter_dict = get_atom_df_mapping(df_col_list, each_atom_args)

            if not all_col_atom_dict:
                all_col_atom_dict.update(col_atom_dict)
            else:
                for k, v in col_atom_dict.items():
                    if k in all_col_atom_dict:
                        temp_lst = []
                        temp_val = all_col_atom_dict[k]
                        if isinstance(temp_val, list):
                            temp_lst = temp_lst + temp_val
                        else:
                            temp_lst.append(temp_val)
                        temp_lst.append(v)
                        all_col_atom_dict[k] = temp_lst
                    else:
                        all_col_atom_dict[k] = v

            if cons_filter_dict:
                atom_df = filter_atom_df(atom_df, cons_filter_dict)

            transformed_df_list.append(atom_df)

    # already have last df
    transformed_df_list.pop()
    # traversing from last to first one by one
    inner_join_counter = 0
    for each_df in reversed(transformed_df_list):
        for k, v in all_col_atom_dict.items():
            if isinstance(v, list):
                cur_val = v.pop()
                for each_col in reversed(v):
                    # handle result df, as having same name of cols when joining
                    atom_df = atom_df.join(each_df, atom_df[cur_val] == each_df[each_col])
                    inner_join_counter += 1
                    break
        if inner_join_counter == 0:
            atom_df = atom_df.join(each_df)

    # convert remaining dict key list to string
    for i, j in all_col_atom_dict.items():
        if isinstance(j, list):
            all_col_atom_dict[i] = "".join(j)

    return atom_df, all_col_atom_dict


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    df1 = spark.read.csv('datasets/students.csv', header=True)
    df2 = spark.read.csv('datasets/student_programs.csv', header=True)
    df3 = spark.read.csv('datasets/student_interest.csv', header=True)

    # declaring Datalog Variables
    x = datalog_ds.DLVariable("X")
    y = datalog_ds.DLVariable("Y")
    z = datalog_ds.DLVariable("Z")
    a = datalog_ds.DLVariable("A")
    t = datalog_ds.DLVariable("T")

    # declaring Datalog Constants
    cs595_c = datalog_ds.DLConstants("cs595")
    mscs_c = datalog_ds.DLConstants("mscs")
    badminton_c = datalog_ds.DLConstants("badminton")

    # Building Datalog Head
    dl_head = datalog_ds.DLAtom("Q", [y, z])

    # declaring Datalog Body
    dl_body_1 = datalog_ds.DLAtom("Student", [x, y, z, a])
    dl_body_2 = datalog_ds.DLAtom("Takes", [x, cs595_c])
    dl_body_3 = datalog_ds.DLAtom("Interest", [x, badminton_c])

    # Building Datalog Body
    obj_rule = datalog_ds.DLRule(dl_head)
    obj_rule.add_body(dl_body_1)
    obj_rule.add_body(dl_body_2)
    # obj_rule.add_body(dl_body_3)

    # Mapping Datalog body to corresponding Dataframes
    # Test 1
    # df_mapping_dict = {"Student": df1, 'Takes': df2, 'Interest': df3}
    # Test 2
    # df_mapping_dict = {"Student": df1}
    # Test 3
    df_mapping_dict = {"Student": df1, 'Takes': df2}

    # Calling main logic functions for transformations
    result_df, all_mapping_dict = get_datalog_result(df_mapping_dict, obj_rule.body)

    result_df.show()
    print(all_mapping_dict)

    # Logic for selecting the columns according to Datalog Head
    select_col_list = []
    head_arg_list = obj_rule.head.args_list
    for each_head_arg in head_arg_list:
        select_col_list.append(all_mapping_dict[each_head_arg])

    result_df = result_df.select(select_col_list)

    # Showing result
    result_df.show()


