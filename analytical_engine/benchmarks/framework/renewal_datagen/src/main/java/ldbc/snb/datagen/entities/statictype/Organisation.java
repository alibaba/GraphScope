

package ldbc.snb.datagen.entities.statictype;

public class Organisation {

    public enum OrganisationType {
        university,
        company
    }

    public long id;
    public String name;
    public OrganisationType type;
    public int location;
}
