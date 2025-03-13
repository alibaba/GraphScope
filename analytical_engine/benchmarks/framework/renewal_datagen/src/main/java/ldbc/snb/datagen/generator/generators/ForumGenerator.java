
package ldbc.snb.datagen.generator.generators;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.person.Person;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Knows;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.util.StringUtils;
import ldbc.snb.datagen.vocabulary.SN;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

public class ForumGenerator {

    public Forum createWall(RandomGeneratorFarm randomFarm, long forumId, Person person) {
        int language = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(person.languages().size());
        Forum forum = new Forum(SN.formId(SN.composeId(forumId, person.creationDate() + DatagenParams.deltaTime)),
                                person.creationDate() + DatagenParams.deltaTime,
                                new Person.PersonSummary(person),
                                StringUtils.clampString("Wall of " + person.firstName() + " " + person.lastName(), 256),
                                person.cityId(),
                                language
        );

        List<Integer> forumTags = new ArrayList<>();
        for (Integer interest : person.interests()) {
            forumTags.add(interest);
        }
        forum.tags(forumTags);

        TreeSet<Knows> knows = person.knows();
        for (Knows k : knows) {
            long date = Math.max(k.creationDate(), forum.creationDate()) + DatagenParams.deltaTime;
            assert (forum
                    .creationDate() + DatagenParams.deltaTime) <= date : "Forum creation date is larger than knows in wall " + forum
                    .creationDate() + " " + k.creationDate();
            forum.addMember(new ForumMembership(forum.id(), date, k.to()));
        }
        return forum;
    }

    public Forum createGroup(RandomGeneratorFarm randomFarm, long forumId, Person person, List<Person> persons) {
        long date = Dictionaries.dates.randomDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), person
                .creationDate() + DatagenParams.deltaTime);

        int language = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(person.languages().size());
        Iterator<Integer> iter = person.interests().iterator();
        int idx = randomFarm.get(RandomGeneratorFarm.Aspect.FORUM_INTEREST).nextInt(person.interests().size());
        for (int i = 0; i < idx; i++) {
            iter.next();
        }
        int interestId = iter.next().intValue();
        List<Integer> interest = new ArrayList<>();
        interest.add(interestId);

        Forum forum = new Forum(SN.formId(SN.composeId(forumId, date)),
                                date,
                                new Person.PersonSummary(person),
                                StringUtils.clampString("Group for " + Dictionaries.tags.getName(interestId)
                                                                                        .replace("\"", "\\\"") + " in " + Dictionaries.places
                                        .getPlaceName(person.cityId()), 256),
                                person.cityId(),
                                language
        );

        // Set tags of this forum
        forum.tags(interest);


        TreeSet<Long> added = new TreeSet<>();
        List<Knows> friends = new ArrayList<>();
        friends.addAll(person.knows());
        int numMembers = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_USERS_PER_FORUM)
                                   .nextInt(DatagenParams.maxNumMemberGroup);
        int numLoop = 0;
        while ((forum.memberships().size() < numMembers) && (numLoop < DatagenParams.blockSize)) {
            double prob = randomFarm.get(RandomGeneratorFarm.Aspect.KNOWS_LEVEL).nextDouble();
            if (prob < 0.3 && person.knows().size() > 0) {
                int friendId = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX).nextInt(person.knows()
                                                                                                         .size());
                Knows k = friends.get(friendId);
                if (!added.contains(k.to().accountId())) {
                    Random random = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX);
                    date = Dictionaries.dates.randomDate(random, Math
                            .max(forum.creationDate(), k.creationDate() + DatagenParams.deltaTime));
                    assert forum
                            .creationDate() + DatagenParams.deltaTime <= date : "Forum creation date larger than membership date for knows based members";
                    forum.addMember(new ForumMembership(forum.id(), date, k.to()));
                    added.add(k.to().accountId());
                }
            } else {
                int candidateIndex = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX)
                                               .nextInt(persons.size());
                Person member = persons.get(candidateIndex);
                prob = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP).nextDouble();
                if ((prob < 0.1) && !added.contains(member.accountId())) {
                    added.add(member.accountId());
                    Random random = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX);
                    date = Dictionaries.dates.randomDate(random,
                                                         Math.max(forum.creationDate(), member
                                                                 .creationDate()) + DatagenParams.deltaTime);
                    assert forum
                            .creationDate() + DatagenParams.deltaTime <= date : "Forum creation date larger than membership date for block based members";
                    forum.addMember(new ForumMembership(forum.id(), date, new Person.PersonSummary(member)));
                    added.add(member.accountId());
                }
            }
            numLoop++;
        }
        return forum;
    }

    public Forum createAlbum(RandomGeneratorFarm randomFarm, long forumId, Person person, int numAlbum) {
        long date = Dictionaries.dates.randomDate(randomFarm.get(RandomGeneratorFarm.Aspect.DATE), person
                .creationDate() + DatagenParams.deltaTime);
        int language = randomFarm.get(RandomGeneratorFarm.Aspect.LANGUAGE).nextInt(person.languages().size());
        Forum forum = new Forum(SN.formId(SN.composeId(forumId, date)),
                                date,
                                new Person.PersonSummary(person),
                                StringUtils.clampString("Album " + numAlbum + " of " + person.firstName() + " " + person
                                        .lastName(), 256),
                                person.cityId(),
                                language
        );

        Iterator<Integer> iter = person.interests().iterator();
        int idx = randomFarm.get(RandomGeneratorFarm.Aspect.FORUM_INTEREST).nextInt(person.interests().size());
        for (int i = 0; i < idx; i++) {
            iter.next();
        }
        int interestId = iter.next().intValue();
        List<Integer> interest = new ArrayList<>();
        interest.add(interestId);
        forum.tags(interest);

        List<Integer> countries = Dictionaries.places.getCountries();
        int randomCountry = randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY).nextInt(countries.size());
        forum.place(countries.get(randomCountry));
        List<Knows> friends = new ArrayList<>();
        friends.addAll(person.knows());
        for (Knows k : friends) {
            double prob = randomFarm.get(RandomGeneratorFarm.Aspect.ALBUM_MEMBERSHIP).nextDouble();
            if (prob < 0.7) {
                Random random = randomFarm.get(RandomGeneratorFarm.Aspect.MEMBERSHIP_INDEX);
                date = Dictionaries.dates.randomDate(random, Math.max(forum.creationDate(), k.creationDate())
                        + DatagenParams.deltaTime);
                forum.addMember(new ForumMembership(forum.id(), date, k.to()));
            }
        }
        return forum;
    }
}
