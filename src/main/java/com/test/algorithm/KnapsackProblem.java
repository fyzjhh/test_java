package com.test.algorithm;

import java.util.Arrays;

import java.util.ArrayList; 

class Knapsack { 
    
    /** 背包重量  */ 
    private int weight; 
       
    /** 背包物品价值  */ 
    private int value; 
    /***
     * 构造器
     */ 
    public Knapsack(int weight, int value) { 
        this.value = value; 
        this.weight = weight; 
    } 
    public int getWeight() { 
        return weight; 
    } 
       
    public int getValue() { 
        return value; 
    } 
       
    public String toString() { 
        return "[weight: " + weight + " " + "value: " + value + "]";   
    } 
}  


public class KnapsackProblem { 
       
    /** 指定背包 */ 
    private Knapsack[] bags; 
       
    /** 总承重  */ 
    private int totalWeight; 
       
    /** 给定背包数量  */ 
    private int n; 
       
    /** 前 n 个背包，总承重为 totalWeight 的最优值矩阵  */ 
    private int[][] bestValues; 
       
    /** 前 n 个背包，总承重为 totalWeight 的最优值 */ 
    private int bestValue; 
       
    /** 前 n 个背包，总承重为 totalWeight 的最优解的物品组成 */ 
    private ArrayList<Knapsack> bestSolution; 
       
    public KnapsackProblem(Knapsack[] bags, int totalWeight) { 
        this.bags = bags; 
        this.totalWeight = totalWeight; 
        this.n = bags.length; 
        if (bestValues == null) { 
            bestValues = new int[n+1][totalWeight+1]; 
        } 
    } 
       
    /**
     * 求解前 n 个背包、给定总承重为 totalWeight 下的背包问题
     * 
     */ 
    public void solve() { 
           
        System.out.println("给定背包："); 
        for(Knapsack b: bags) { 
            System.out.println(b); 
        } 
        System.out.println("给定总承重: " + totalWeight); 
           
        // 求解最优值 
        for (int j = 0; j <= totalWeight; j++) { 
            for (int i = 0; i <= n; i++) { 
               
                if (i == 0 || j == 0) { 
                    bestValues[i][j] = 0; 
                }    
                else  
                { 
                    // 如果第 i 个背包重量大于总承重，则最优解存在于前 i-1 个背包中， 
                    // 注意：第 i 个背包是 bags[i-1] 
                    if (j < bags[i-1].getWeight()) { 
                        bestValues[i][j] = bestValues[i-1][j]; 
                    }    
                    else  
                    { 
                        // 如果第 i 个背包不大于总承重，则最优解要么是包含第 i 个背包的最优解， 
                        // 要么是不包含第 i 个背包的最优解， 取两者最大值，这里采用了分类讨论法 
                        // 第 i 个背包的重量 iweight 和价值 ivalue 
                        int iweight = bags[i-1].getWeight(); 
                        int ivalue = bags[i-1].getValue(); 
                        bestValues[i][j] =  
                            Math.max(bestValues[i-1][j], ivalue + bestValues[i-1][j-iweight]);       
                    } // else 
                } //else          
           } //for 
        } //for 
           
        // 求解背包组成 
        if (bestSolution == null) { 
            bestSolution = new ArrayList<Knapsack>(); 
        } 
        int tempWeight = totalWeight; 
        for (int i=n; i >= 1; i--) { 
           if (bestValues[i][tempWeight] > bestValues[i-1][tempWeight]) { 
               bestSolution.add(bags[i-1]);  // bags[i-1] 表示第 i 个背包 
               tempWeight -= bags[i-1].getWeight(); 
           } 
           if (tempWeight == 0) { break; } 
        } 
        bestValue = bestValues[n][totalWeight]; 
    } 
       
    /**
     * 获得前  n 个背包， 总承重为 totalWeight 的背包问题的最优解值
     * 调用条件： 必须先调用 solve 方法
     * 
     */ 
    public int getBestValue() {  
        return bestValue; 
    } 
       
    /**
     * 获得前  n 个背包， 总承重为 totalWeight 的背包问题的最优解值矩阵
     * 调用条件： 必须先调用 solve 方法
     * 
     */ 
    public int[][] getBestValues() { 
           
        return bestValues; 
    } 
       
    /**
     * 获得前  n 个背包， 总承重为 totalWeight 的背包问题的最优解值矩阵
     * 调用条件： 必须先调用 solve 方法
     * 
     */ 
    public ArrayList<Knapsack> getBestSolution() { 
        return bestSolution; 
    } 
       
    public static void main(String[] args) { 
        
        Knapsack[] bags = new Knapsack[] { 
                new Knapsack(2,13), new Knapsack(1,10), 
                new Knapsack(3,24), new Knapsack(2,15), 
                new Knapsack(4,28), new Knapsack(5,33), 
                new Knapsack(3,20), new Knapsack(1, 8) 
        }; 
        int totalWeight = 12; 
        KnapsackProblem kp = new KnapsackProblem(bags, totalWeight); 
           
        kp.solve(); 
        System.out.println(" -------- 该背包问题实例的解: --------- "); 
        System.out.println("最优值：" + kp.getBestValue());  
        System.out.println("最优解【选取的背包】: "); 
        System.out.println(kp.getBestSolution()); 
        System.out.println("最优值矩阵："); 
        int[][] bestValues = kp.getBestValues(); 
        for (int i=0; i < bestValues.length; i++) { 
            for (int j=0; j < bestValues[i].length; j++) { 
                System.out.printf("%-5d", bestValues[i][j]); 
            } 
            System.out.println(); 
        } 
    } 

} 
 
