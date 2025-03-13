package ldbc.snb.datagen.entities.statictype.place;

public class PopularPlace {

    private String name;
    private double latt;
    private double longt;

    public PopularPlace(String _name, double _latt, double _longt) {
        this.name = _name;
        this.latt = _latt;
        this.longt = _longt;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getLatt() {
        return latt;
    }

    public double getLongt() {
        return longt;
    }

}
