package ldbc.snb.datagen.entities.statictype.place;

import java.io.Serializable;

@SuppressWarnings("serial")
public class Place implements Serializable {

    public static final String CITY = "city";
    public static final String COUNTRY = "country";
    public static final String CONTINENT = "continent";
    public static final String AREA = "world";

    private int id;
    private int zId;

    private String name;
    private double latt;
    private double longt;
    private long population;
    private String type;

    public int getzId() {
        return zId;
    }

    public void setzId(int zId) {
        this.zId = zId;
    }

    public Place() {
    }

    public Place(int _id, String _name, double _longt, double _latt, int _population, String _type) {
        this.id = _id;
        this.name = _name;
        this.longt = _longt;
        this.latt = _latt;
        this.population = _population;
        this.type = _type;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getLongt() {
        return longt;
    }

    public void setLongt(double longt) {
        this.longt = longt;
    }

    public double getLatt() {
        return latt;
    }

    public void setLatt(double latt) {
        this.latt = latt;
    }

    public long getPopulation() {
        return population;
    }

    public void setPopulation(long population) {
        this.population = population;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

}
