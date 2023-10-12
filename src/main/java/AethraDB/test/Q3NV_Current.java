package AethraDB.test;

import AethraDB.evaluation.codegen.infrastructure.data.ABQArrowTableReader;
import AethraDB.evaluation.codegen.infrastructure.data.ArrowTableReader;
import AethraDB.evaluation.general_support.ArrowOptimisations;
import AethraDB.evaluation.general_support.hashmaps.Int_Hash_Function;
import AethraDB.test.Q3NV_Current_Support.KeyValueMap_1873859565;
import AethraDB.test.Q3NV_Current_Support.KeyMultiRecordMap_388043093;
import AethraDB.test.Q3NV_Current_Support.KeyMultiRecordMap_811760110;
import org.apache.arrow.memory.RootAllocator;

import java.io.File;
import java.util.Arrays;

public class Q3NV_Current {

    public static void main(String[] args) throws Exception {
        String tableFilePath = args[0];
        RootAllocator rootAllocator = new RootAllocator();
        ABQArrowTableReader customer = new ABQArrowTableReader(new File(tableFilePath + "/customer.arrow"), rootAllocator, true, new int[] { 0, 6 });
        ABQArrowTableReader orders = new ABQArrowTableReader(new File(tableFilePath + "/orders.arrow"), rootAllocator, true, new int[] { 0, 1, 4, 7 });
        ABQArrowTableReader lineitem = new ABQArrowTableReader(new File(tableFilePath + "/lineitem.arrow"), rootAllocator, true, new int[] { 0, 5, 6, 10 });

        long start = System.nanoTime();

        query(customer, orders, lineitem);

        long end = System.nanoTime();
        long msDuration = (end - start) / 1_000_000;
        System.out.println(msDuration);
    }

    public static void query(ABQArrowTableReader customer, ABQArrowTableReader orders, ABQArrowTableReader lineitem) {
        /// GENERATED
        byte[] byte_array_cache = null;
        long result_count = 0;
        KeyValueMap_1873859565 aggregation_state_map = new KeyValueMap_1873859565();
        KeyMultiRecordMap_388043093 join_map = new KeyMultiRecordMap_388043093();
        KeyMultiRecordMap_811760110 join_map_0 = new KeyMultiRecordMap_811760110();

//        ArrowTableReader customer = cCtx.getArrowReader(0);
        while (customer.loadNextBatch()) {
            org.apache.arrow.vector.IntVector customer_vc_0 = ((org.apache.arrow.vector.IntVector) customer.getVector(0));
            org.apache.arrow.vector.FixedSizeBinaryVector customer_vc_1 = ((org.apache.arrow.vector.FixedSizeBinaryVector) customer.getVector(6));
            int recordCount = customer_vc_0.getValueCount();
            for (int aviv = 0; aviv < recordCount; aviv++) {
                byte_array_cache = ArrowOptimisations.getFixedSizeBinaryValue(customer_vc_1, aviv, byte_array_cache);
                if (!(Arrays.equals(byte_array_cache, new byte[] { 66, 85, 73, 76, 68, 73, 78, 71, 32, 32 }))) {
                    continue;
                }
                int ordinal_value = customer_vc_0.get(aviv);
                long left_join_key_prehash = Int_Hash_Function.preHash(ordinal_value);
                join_map_0.associate(ordinal_value, left_join_key_prehash);
            }
        }
//        ArrowTableReader orders = cCtx.getArrowReader(1);
        while (orders.loadNextBatch()) {
            org.apache.arrow.vector.IntVector orders_vc_0 = ((org.apache.arrow.vector.IntVector) orders.getVector(0));
            org.apache.arrow.vector.IntVector orders_vc_1 = ((org.apache.arrow.vector.IntVector) orders.getVector(1));
            org.apache.arrow.vector.DateDayVector orders_vc_2 = ((org.apache.arrow.vector.DateDayVector) orders.getVector(4));
            org.apache.arrow.vector.IntVector orders_vc_3 = ((org.apache.arrow.vector.IntVector) orders.getVector(7));
            int recordCount = orders_vc_0.getValueCount();
            for (int aviv = 0; aviv < recordCount; aviv++) {
                int ordinal_value = orders_vc_2.get(aviv);
                if (!((ordinal_value < 9204))) {
                    continue;
                }
                int ordinal_value_0 = orders_vc_1.get(aviv);
                long right_join_key_prehash = Int_Hash_Function.preHash(ordinal_value_0);
                KeyMultiRecordMap_811760110.KeyRecordType records_to_join_descr = join_map_0.getIndex(ordinal_value_0, right_join_key_prehash);
                if ((records_to_join_descr == null)) {
                    continue;
                }
                int ordinal_value_1 = orders_vc_0.get(aviv);
                int ordinal_value_2 = orders_vc_3.get(aviv);
                int left_join_record_count = records_to_join_descr.count;
                for (int i = 0; i < left_join_record_count; i++) {
                    long left_join_key_prehash = Int_Hash_Function.preHash(ordinal_value_1);
                    join_map.associate(ordinal_value_1, left_join_key_prehash, ordinal_value, ordinal_value_2);
                }
            }
        }
//        ArrowTableReader lineitem = cCtx.getArrowReader(2);
        while (lineitem.loadNextBatch()) {
            org.apache.arrow.vector.IntVector lineitem_vc_0 = ((org.apache.arrow.vector.IntVector) lineitem.getVector(0));
            org.apache.arrow.vector.Float8Vector lineitem_vc_1 = ((org.apache.arrow.vector.Float8Vector) lineitem.getVector(5));
            org.apache.arrow.vector.Float8Vector lineitem_vc_2 = ((org.apache.arrow.vector.Float8Vector) lineitem.getVector(6));
            org.apache.arrow.vector.DateDayVector lineitem_vc_3 = ((org.apache.arrow.vector.DateDayVector) lineitem.getVector(10));
            int recordCount = lineitem_vc_0.getValueCount();
            for (int aviv = 0; aviv < recordCount; aviv++) {
                int ordinal_value = lineitem_vc_3.get(aviv);
                if (!((ordinal_value > 9204))) {
                    continue;
                }
                int projection_literal = 1;
                double ordinal_value_0 = lineitem_vc_2.get(aviv);
                double projection_computation_result = (projection_literal - ordinal_value_0);
                double ordinal_value_1 = lineitem_vc_1.get(aviv);
                double projection_computation_result_0 = (ordinal_value_1 * projection_computation_result);
                int ordinal_value_2 = lineitem_vc_0.get(aviv);
                long right_join_key_prehash = Int_Hash_Function.preHash(ordinal_value_2);
                KeyMultiRecordMap_388043093.KeyRecordType records_to_join_descr = join_map.getIndex(ordinal_value_2, right_join_key_prehash);
                if ((records_to_join_descr == null)) {
                    continue;
                }
                int left_join_record_count = records_to_join_descr.count;
                for (int i = 0; i < left_join_record_count; i++) {
                    KeyMultiRecordMap_388043093.ValueRecordType left_join_rec = records_to_join_descr.records[i];
                    int left_join_ord_0 = left_join_rec.value_ord_0;
                    int left_join_ord_1 = left_join_rec.value_ord_1;
                    long group_key_pre_hash = Int_Hash_Function.preHash(ordinal_value_2);
                    group_key_pre_hash ^= Int_Hash_Function.preHash(left_join_ord_0);
                    group_key_pre_hash ^= Int_Hash_Function.preHash(left_join_ord_1);
                    aggregation_state_map.incrementForKey(ordinal_value_2, left_join_ord_0, left_join_ord_1, group_key_pre_hash, projection_computation_result_0);
                }
            }
        }
        for (int key_i = 0; key_i < aggregation_state_map.numberOfRecords; key_i++) {
            KeyValueMap_1873859565.RecordType currentRecord = aggregation_state_map.records[key_i];
            int groupKey_0 = currentRecord.key_ord_0;
            int groupKey_1 = currentRecord.key_ord_1;
            int groupKey_2 = currentRecord.key_ord_2;
            double aggregation_0_value = currentRecord.value_ord_0;
            result_count++;
        }
        System.out.println(result_count);
        /// END GENERATED
    }

}
