package ldbc.snb.datagen.generator.generators;

import ldbc.snb.datagen.DatagenParams;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.entities.dynamic.Forum;
import ldbc.snb.datagen.entities.dynamic.messages.Photo;
import ldbc.snb.datagen.entities.dynamic.person.IP;
import ldbc.snb.datagen.entities.dynamic.relations.ForumMembership;
import ldbc.snb.datagen.entities.dynamic.relations.Like;
import ldbc.snb.datagen.entities.statictype.place.PopularPlace;
import ldbc.snb.datagen.serializer.PersonActivityExporter;
import ldbc.snb.datagen.util.PersonBehavior;
import ldbc.snb.datagen.util.RandomGeneratorFarm;
import ldbc.snb.datagen.vocabulary.SN;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

public class PhotoGenerator {
    private LikeGenerator likeGenerator_;
    private Photo photo_;


    public PhotoGenerator(LikeGenerator likeGenerator) {
        this.likeGenerator_ = likeGenerator;
        this.photo_ = new Photo();
    }

    public long createPhotos(RandomGeneratorFarm randomFarm, final Forum album, final List<ForumMembership> memberships, long numPhotos, long startId, PersonActivityExporter exporter) throws IOException {
        long nextId = startId;
        int numPopularPlaces = randomFarm.get(RandomGeneratorFarm.Aspect.NUM_POPULAR)
                                         .nextInt(DatagenParams.maxNumPopularPlaces + 1);
        List<Short> popularPlaces = new ArrayList<>();
        for (int i = 0; i < numPopularPlaces; i++) {
            short aux = Dictionaries.popularPlaces.getPopularPlace(randomFarm
                                                                           .get(RandomGeneratorFarm.Aspect.POPULAR), album
                                                                           .place());
            if (aux != -1) {
                popularPlaces.add(aux);
            }
        }
        for (int i = 0; i < numPhotos; ++i) {
            int locationId = album.place();
            double latt = 0;
            double longt = 0;
            if (popularPlaces.size() == 0) {
                latt = Dictionaries.places.getLatt(locationId);
                longt = Dictionaries.places.getLongt(locationId);
            } else {
                int popularPlaceId;
                PopularPlace popularPlace;
                if (randomFarm.get(RandomGeneratorFarm.Aspect.POPULAR).nextDouble() < DatagenParams.probPopularPlaces) {
                    // Generate photo information from user's popular place
                    int popularIndex = randomFarm.get(RandomGeneratorFarm.Aspect.POPULAR).nextInt(popularPlaces.size());
                    popularPlaceId = popularPlaces.get(popularIndex);
                    popularPlace = Dictionaries.popularPlaces.getPopularPlace(album.place(), popularPlaceId);
                    latt = popularPlace.getLatt();
                    longt = popularPlace.getLongt();
                } else {
                    // Randomly select one places from Album location idx
                    popularPlaceId = Dictionaries.popularPlaces.getPopularPlace(randomFarm
                                                                                        .get(RandomGeneratorFarm.Aspect.POPULAR), locationId);
                    if (popularPlaceId != -1) {
                        popularPlace = Dictionaries.popularPlaces.getPopularPlace(locationId, popularPlaceId);
                        latt = popularPlace.getLatt();
                        longt = popularPlace.getLongt();
                    } else {
                        latt = Dictionaries.places.getLatt(locationId);
                        longt = Dictionaries.places.getLongt(locationId);
                    }
                }
            }
            TreeSet<Integer> tags = new TreeSet<>();
            long date = album.creationDate() + DatagenParams.deltaTime + 1000 * (i + 1);
            int country = album.moderator().countryId();
            IP ip = album.moderator().ipAddress();
            Random random = randomFarm.get(RandomGeneratorFarm.Aspect.DIFF_IP_FOR_TRAVELER);
            if (PersonBehavior.changeUsualCountry(random, date)) {
                random = randomFarm.get(RandomGeneratorFarm.Aspect.COUNTRY);
                country = Dictionaries.places.getRandomCountryUniform(random);
                random = randomFarm.get(RandomGeneratorFarm.Aspect.IP);
                ip = Dictionaries.ips.getIP(random, country);
            }

            long id = SN.formId(SN.composeId(nextId++, date));
            photo_.initialize(id,
                              date,
                              album.moderator(),
                              album.id(),
                              "photo" + id + ".jpg",
                              tags,
                              country,
                              ip,
                              album.moderator().browserId(),
                              latt,
                              longt);
            exporter.export(photo_);
            if (randomFarm.get(RandomGeneratorFarm.Aspect.NUM_LIKE).nextDouble() <= 0.1) {
                likeGenerator_.generateLikes(randomFarm
                                                     .get(RandomGeneratorFarm.Aspect.NUM_LIKE), album, photo_, Like.LikeType.PHOTO, exporter);
            }
        }
        return nextId;
    }

}
