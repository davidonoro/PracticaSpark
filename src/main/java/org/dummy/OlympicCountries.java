package org.dummy;

import java.io.Serializable;

public class OlympicCountries implements Serializable{
    private final String name;
    private final int yearFirstMedal;
    private final int yearLastMedal;
    private final int avgGoldMedals;
    private final int maxGoldMedals;
    private final int totGoldMedals;
    private final int avgSilverMedals;
    private final int maxSilverMedals;
    private final int totSilverMedals;
    private final int avgBronzeMedals;
    private final int maxBronzeMedals;
    private final int totBronzeMedals;

    public OlympicCountries(String name, int yearFirstMedal, int yearLastMedal, int avgGoldMedals, int maxGoldMedals,
                            int totGoldMedals, int avgSilverMedals, int maxSilverMedals, int totSilverMedals,
                            int avgBronzeMedals, int maxBronzeMedals, int totBronzeMedals) {
        this.name = name;
        this.yearFirstMedal = yearFirstMedal;
        this.yearLastMedal = yearLastMedal;
        this.avgGoldMedals = avgGoldMedals;
        this.maxGoldMedals = maxGoldMedals;
        this.totGoldMedals = totGoldMedals;
        this.avgSilverMedals = avgSilverMedals;
        this.maxSilverMedals = maxSilverMedals;
        this.totSilverMedals = totSilverMedals;
        this.avgBronzeMedals = avgBronzeMedals;
        this.maxBronzeMedals = maxBronzeMedals;
        this.totBronzeMedals = totBronzeMedals;
    }

    public String getName() {
        return name;
    }

    public int getYearFirstMedal() {
        return yearFirstMedal;
    }

    public int getYearLastMedal() {
        return yearLastMedal;
    }

    public int getAvgGoldMedals() {
        return avgGoldMedals;
    }

    public int getMaxGoldMedals() {
        return maxGoldMedals;
    }

    public int getTotGoldMedals() {
        return totGoldMedals;
    }

    public int getAvgSilverMedals() {
        return avgSilverMedals;
    }

    public int getMaxSilverMedals() {
        return maxSilverMedals;
    }

    public int getTotSilverMedals() {
        return totSilverMedals;
    }

    public int getAvgBronzeMedals() {
        return avgBronzeMedals;
    }

    public int getMaxBronzeMedals() {
        return maxBronzeMedals;
    }

    public int getTotBronzeMedals() {
        return totBronzeMedals;
    }


}
