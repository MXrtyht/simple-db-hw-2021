package simpledb.optimizer;

import java.util.Map;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    // how many buckets
    private int buckets;
    private int min;
    private int max;
    // bucket ranges
    private int width;
    private int[] bucket;
    private int totalValues;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.width = ((max - min + 1) / this.buckets) + ((max - min + 1)%this.buckets == 0 ? 0 : 1) ;
        this.bucket = new int[buckets];
        this.totalValues = 0;
    }

    private int bucketIndex(int value){
        int clampedValue = Math.max(min, Math.min(max, value));
        return (clampedValue - min) / this.width;
    }

    private int bucketLeft(int bucketIndex){
        return bucketIndex * this.width + this.min;
    }

    private int bucketRight(int bucketIndex){
        return this.bucketLeft(bucketIndex) + this.width - 1 ;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int index = bucketIndex(v);
        if(index >= 0 && index < buckets){
            this.bucket[bucketIndex(v)]++;
            this.totalValues ++;
        }
    }

    private double estimateEqual(int index){
        int h = this.bucket[index];
        int w = this.bucketRight(index) - this.bucketLeft(index) + 1;
        return ((double)h / (double)w) / (double)this.totalValues;
    }

    private double estimateLessThan(int value, int index){
        double selectity = 0.0;

        if(value > this.max){
            return 1.0;
        }

        // 当前桶中 < value 的部分
        int b_left = bucketLeft(index);
        int h = this.bucket[index];
        int w = bucketRight(index) - bucketLeft(index) + 1;
        selectity += ((double)(value - b_left) / (double)w) * ((double) h / (double)this.totalValues);

        // 前面的所有桶
        for(int i=0; i < index; i++){
            selectity += (double) bucket[i] / this.totalValues;
        }
        return selectity;
    }

    private double estimateGreaterThan(int value, int index){
        double selectity = 0.0;

        if(value < this.min){
            return 1.0;
        }

        // 当前桶中 > value 的部分
        int b_right = bucketRight(index);
        int h = this.bucket[index];
        int w = bucketRight(index) - bucketLeft(index) + 1;

        selectity += ((double)(b_right - value) / (double)w) * ((double) h / (double)this.totalValues);
        // 后面所有桶
        for(int i=index+1; i < buckets; i++){
            selectity += (double) bucket[i] / this.totalValues;
        }

        return selectity;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        int index = bucketIndex(v);
        double result;
        switch (op) {
            case EQUALS:
                return this.estimateEqual(index);
            case LESS_THAN:
                return this.estimateLessThan(v, index);
            case LESS_THAN_OR_EQ:
                result = this.estimateLessThan(v, index) + this.estimateEqual(index);
                return  result > 1.0 ? 1.0 : result;
            case GREATER_THAN:
                return this.estimateGreaterThan(v, index);
            case GREATER_THAN_OR_EQ:
                result = this.estimateGreaterThan(v, index) + this.estimateEqual(index);
                return  result > 1.0 ? 1.0 : result;
            case NOT_EQUALS:
                return 1 - this.estimateEqual(index);
            default:
                return 0.0;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        if(this.totalValues == 0) return 1.0;

        double totalSelectivity = 0;
        for(int i=0; i<buckets; i++){
            totalSelectivity += (double)bucket[i] / totalValues;
        }
        return (double)totalSelectivity / (double)buckets;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        sb.append("IntHistogram(buckets=").append(buckets);
        sb.append(", min=").append(min);
        sb.append(", max=").append(max);
        sb.append(", totalValues=").append(totalValues);
        sb.append(")\n");
        
        for (int i = 0; i < buckets; i++) {
            sb.append(String.format("  Bucket %d: [%d, %d] = %d\n", 
                i, bucketLeft(i), bucketRight(i), bucket[i]));
        }
        
        return sb.toString();
    }
}
