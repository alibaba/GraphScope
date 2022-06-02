package com.alibaba.graphscope.integration.ldbc;

import com.alibaba.graphscope.gremlin.plugin.traversal.IrCustomizedTraversal;
import com.google.common.collect.Sets;

import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class LdbcQueryTest extends AbstractGremlinProcessTest {
    public abstract Traversal<Vertex, Map<String, Object>> get_ldbc_1_test();

    public abstract Traversal<Vertex, Map<String, Object>> get_ldbc_2_test();

    public abstract Traversal<Vertex, Vertex> get_ldbc_3_test();

    public abstract Traversal<Vertex, Map<String, Object>> get_ldbc_4_test();

    public abstract Traversal<Vertex, Map<Object, Long>> get_ldbc_5_test();

    public abstract Traversal<Vertex, Map<String, Object>> get_ldbc_6_test();

    public abstract Traversal<Vertex, Map<String, Object>> get_ldbc_7_test();

    public abstract Traversal<Vertex, Map<String, Object>> get_ldbc_8_test();

    public abstract Traversal<Vertex, Map<String, Object>> get_ldbc_9_test();

    public abstract Traversal<Vertex, Map<String, Object>> get_ldbc_11_test();

    public abstract Traversal<Vertex, Map<Object, Long>> get_ldbc_12_test();

    @Test
    public void run_ldbc_1_test() {
        Traversal<Vertex, Map<String, Object>> traversal = this.get_ldbc_1_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        List<String> expected =
                Arrays.asList(
                        "{p=2, a={firstName=[Chau], lastName=[Nguyen], id=[4848]}}",
                        "{p=2, a={firstName=[Chau], lastName=[Do], id=[9101]}}",
                        "{p=2, a={firstName=[Chau], lastName=[Nguyen], id=[2199023265573]}}",
                        "{p=2, a={firstName=[Chau], lastName=[Loan], id=[6597069771031]}}",
                        "{p=2, a={firstName=[Chau], lastName=[Nguyen], id=[8796093031224]}}",
                        "{p=2, a={firstName=[Chau], lastName=[Ho], id=[10995116285282]}}",
                        "{p=2, a={firstName=[Chau], lastName=[Loan], id=[13194139544258]}}",
                        "{p=2, a={firstName=[Chau], lastName=[Ha], id=[15393162793500]}}",
                        "{p=2, a={firstName=[Chau], lastName=[Ho], id=[19791209303405]}}",
                        "{p=2, a={firstName=[Chau], lastName=[Nguyen], id=[26388279068635]}}",
                        "{p=2, a={firstName=[Chau], lastName=[Loan], id=[26388279076217]}}",
                        "{p=2, a={firstName=[Chau], lastName=[Nguyen], id=[28587302322743]}}",
                        "{p=2, a={firstName=[Chau], lastName=[Ho], id=[28587302323020]}}",
                        "{p=2, a={firstName=[Chau], lastName=[Ho], id=[32985348842021]}}",
                        "{p=3, a={firstName=[Chau], lastName=[Loan], id=[10995116284332]}}",
                        "{p=3, a={firstName=[Chau], lastName=[Ha], id=[15393162789090]}}",
                        "{p=3, a={firstName=[Chau], lastName=[Nguyen], id=[26388279072379]}}",
                        "{p=3, a={firstName=[Chau], lastName=[Ho], id=[32985348840129]}}");

        while (traversal.hasNext()) {
            Map<String, Object> bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected.get(counter)));
            ++counter;
        }

        Assert.assertEquals(expected.size(), (long) counter);
    }

    @Test
    public void run_ldbc_2_test() {
        Traversal<Vertex, Map<String, Object>> traversal = this.get_ldbc_2_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        List<String> expected =
                Arrays.asList(
                        "{p={lastName=[Khan], firstName=[Kunal], id=[30786325587937]},"
                                + " m={id=[2061587339072], creationDate=[20120803072025654],"
                                + " content=[fine]}}",
                        "{p={lastName=[Rao], firstName=[Abhishek], id=[8796093031885]},"
                                + " m={id=[2061587080174], creationDate=[20120803070839866],"
                                + " content=[About Paul Martin, aham for the inteAbout Ernest"
                                + " Hemingway,  which he wrote FAbo]}}",
                        "{p={lastName=[Khan], firstName=[John], id=[8796093029267]},"
                            + " m={id=[2061585035664], creationDate=[20120803070658893],"
                            + " content=[About Amitabh Bachchan, l. In additAbout Chen Shui-bian,"
                            + " en Yi-hsiunAbout Denzel Wash]}}",
                        "{p={lastName=[Khan], firstName=[John], id=[8796093029267]},"
                            + " m={imageFile=[], id=[2061587033910],"
                            + " creationDate=[20120803070442531], content=[About Abbas I of Persia,"
                            + " took back land from the Portuguese and the Mughals. Abbas was a"
                            + " great builder and moved his kingdom's capital from Qazvin to"
                            + " Isfahan. In his later years, the shah became suspicious of his own"
                            + " so]}}",
                        "{p={lastName=[Khan], firstName=[Arjun], id=[30786325579845]},"
                                + " m={id=[2061589148849], creationDate=[20120803065019796],"
                                + " content=[thanks]}}",
                        "{p={lastName=[Rao], firstName=[Abhishek], id=[8796093031885]},"
                            + " m={id=[2061590844627], creationDate=[20120803064241072],"
                            + " content=[About Thomas Aquinas, he Great Books and seminar method."
                            + " It haAbout Edward Elga]}}",
                        "{p={lastName=[Khan], firstName=[John], id=[8796093029267]},"
                                + " m={id=[2061585035666], creationDate=[20120803064146457],"
                                + " content=[great]}}",
                        "{p={lastName=[Bolier], firstName=[Albert], id=[8796093031407]},"
                                + " m={id=[2061588995483], creationDate=[20120803063619558],"
                                + " content=[thx]}}",
                        "{p={lastName=[Rao], firstName=[Abhishek], id=[8796093031885]},"
                            + " m={id=[2061587080596], creationDate=[20120803062639721],"
                            + " content=[About Walt Whitman, n was concernAbout At Fillmore East,"
                            + " 500 Greatest About Italy, Coun]}}",
                        "{p={lastName=[Khan], firstName=[Shweta], id=[7725]},"
                                + " m={id=[2061588924543], creationDate=[20120803061904810],"
                                + " content=[great]}}",
                        "{p={lastName=[Rao], firstName=[Arjun], id=[10995116279390]},"
                            + " m={id=[2061585663942], creationDate=[20120803061330637],"
                            + " content=[About William the Conqueror, ed, but William was able to"
                            + " put them dowAbout Anne]}}",
                        "{p={lastName=[Garcia], firstName=[Isabel], id=[10995116286316]},"
                                + " m={id=[2061590620731], creationDate=[20120803060002996],"
                                + " content=[maybe]}}",
                        "{p={lastName=[Khan], firstName=[Arjun], id=[24189255811940]},"
                            + " m={id=[2061586499110], creationDate=[20120803053430934],"
                            + " content=[About Samuel Johnson, t, literary critic, biAbout Francis"
                            + " Bacon,  proper methodology ]}}",
                        "{p={lastName=[Zhang], firstName=[Yang], id=[13194139535025]},"
                                + " m={id=[2061588568513], creationDate=[20120803050920738],"
                                + " content=[About Harry S. Truman, edom and human rights seemAbout"
                                + " John Steinbeck, and E]}}",
                        "{p={lastName=[Kumar], firstName=[Deepak], id=[17592186053700]},"
                                + " m={id=[2061588703136], creationDate=[20120803045119016],"
                                + " content=[great]}}",
                        "{p={lastName=[Garcia], firstName=[Isabel], id=[10995116286316]},"
                                + " m={id=[2061587017186], creationDate=[20120803043821969],"
                                + " content=[ok]}}",
                        "{p={lastName=[Rao], firstName=[Arjun], id=[10995116279390]},"
                                + " m={id=[2061584412526], creationDate=[20120803043634096],"
                                + " content=[great]}}",
                        "{p={lastName=[Zhang], firstName=[Yang], id=[13194139535025]},"
                                + " m={id=[2061589148863], creationDate=[20120803034926704],"
                                + " content=[ok]}}",
                        "{p={lastName=[Rao], firstName=[Arjun], id=[10995116279390]},"
                            + " m={id=[2061585353358], creationDate=[20120803033714516],"
                            + " content=[About Chen Shui-bian,  ended more than fiAbout Henry Clay,"
                            + " grams. In 1957,]}}",
                        "{p={lastName=[Rao], firstName=[Abhishek], id=[8796093031885]},"
                            + " m={id=[2061587080731], creationDate=[20120803033107843],"
                            + " content=[About Augustine of Hippo, ity of God, distincAbout Harold"
                            + " Arlen, , a numbe]}}");

        while (traversal.hasNext()) {
            Map<String, Object> bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected.get(counter)));
            ++counter;
        }

        Assert.assertEquals(expected.size(), (long) counter);
    }

    @Test
    public void run_ldbc_3_test() {
        Traversal<Vertex, Vertex> traversal = this.get_ldbc_3_test();
        this.printTraversalForm(traversal);
        // V[72066390130957625]: id: 8796093029689, firstName: Eun-Hye, lastName: Yoon
        Assert.assertEquals(72066390130957625L, traversal.next().id());
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void run_ldbc_4_test() {
        Traversal<Vertex, Map<String, Object>> traversal = this.get_ldbc_4_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        List<String> expected =
                Arrays.asList(
                        "{postCount=3, tagName=Ehud_Olmert}",
                        "{postCount=1, tagName=Be-Bop-A-Lula}",
                        "{postCount=1, tagName=Kingdom_of_Sardinia}",
                        "{postCount=1, tagName=The_Singles:_The_First_Ten_Years}");

        while (traversal.hasNext()) {
            Map<String, Object> bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected.get(counter)));
            ++counter;
        }

        Assert.assertEquals(expected.size(), (long) counter);
    }

    @Test
    public void run_ldbc_5_test() {
        Traversal<Vertex, Map<Object, Long>> traversal = this.get_ldbc_5_test();
        this.printTraversalForm(traversal);

        String expected =
                "{v[288230788468638061]=2, v[288231613102324388]=2, v[288231887980212232]=2,"
                    + " v[288230788468574546]=1, v[288230788468598862]=1, v[288230925907600292]=1,"
                    + " v[288230925907605637]=1, v[288231338224392113]=1, v[288231475663357917]=1,"
                    + " v[288231475663417090]=1, v[288231750541328016]=1, v[288231887980204174]=1,"
                    + " v[288231887981252988]=1, v[288232162858188293]=1, v[288232162858200268]=1,"
                    + " v[288232300297113293]=1, v[288232300297120851]=1, v[288232437736032472]=1,"
                    + " v[288232437736038238]=1}";
        Map<Object, Long> result = traversal.next();
        Assert.assertEquals(expected, result.toString());
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void run_ldbc_6_test() {
        Traversal<Vertex, Map<String, Object>> traversal = this.get_ldbc_6_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        List<String> expected =
                Arrays.asList(
                        "{keys=Tom_Gehrels, values=28}",
                        "{keys=Sammy_Sosa, values=9}",
                        "{keys=Charles_Dickens, values=5}",
                        "{keys=Genghis_Khan, values=5}",
                        "{keys=Ivan_Ljubičić, values=5}",
                        "{keys=Marc_Gicquel, values=5}",
                        "{keys=Freddie_Mercury, values=4}",
                        "{keys=Peter_Hain, values=4}",
                        "{keys=Robert_Fripp, values=4}",
                        "{keys=Boris_Yeltsin, values=3}");
        while (traversal.hasNext()) {
            Map<String, Object> bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected.get(counter)));
            ++counter;
        }

        Assert.assertEquals(expected.size(), (long) counter);
    }

    @Test
    public void run_ldbc_7_test() {
        Traversal<Vertex, Map<String, Object>> traversal = this.get_ldbc_7_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        List<String> expected =
                Arrays.asList(
                        "{likedate=20120912143240024, liker={firstName=[Jean-Pierre],"
                            + " lastName=[Kanam], id=[17592186049473]},"
                            + " message={id=[2199024319581], content=[About Charles V, Holy Roman"
                            + " Emperor, rmation. In addition to thAbout Bob Dyla]}}",
                        "{likedate=20120911131917097, liker={firstName=[Ajuma], lastName=[Leakey],"
                            + " id=[32985348842700]}, message={id=[1786707240991], content=[About"
                            + " Costa Rica, a to the southeast, the Pacific Ocean to theAbout"
                            + " Napoleon III, pula]}}",
                        "{likedate=20120910053218016, liker={firstName=[Mohamed], lastName=[Wong],"
                            + " id=[32985348835903]}, message={id=[1786706525707], content=[About"
                            + " Christopher Lee, strong voice and imposing height. He has performed"
                            + " ro]}}",
                        "{likedate=20120905054751531, liker={firstName=[Alfonso],"
                            + " lastName=[Gonzalez], id=[32985348842314]},"
                            + " message={id=[1786707314163], content=[About Anne, Queen of Great"
                            + " Britain,  the resAbout Bob Dylan, ed in MaAbout Frank Zappa]}}",
                        "{likedate=20120904190103709, liker={firstName=[Sirak], lastName=[Dego],"
                            + " id=[32985348835356]}, message={id=[1511833096614], content=[About"
                            + " Gustav Mahler,  (Hofoper). DAbout Karl Marx, list politicaAbout"
                            + " Pablo Picasso, a ]}}",
                        "{likedate=20120904154553202, liker={firstName=[Charles], lastName=[Bona],"
                            + " id=[32985348837885]}, message={id=[1786707240991], content=[About"
                            + " Costa Rica, a to the southeast, the Pacific Ocean to theAbout"
                            + " Napoleon III, pula]}}",
                        "{likedate=20120904131816796, liker={firstName=[Aditya], lastName=[Khan],"
                            + " id=[32985348841531]}, message={id=[1786707314163], content=[About"
                            + " Anne, Queen of Great Britain,  the resAbout Bob Dylan, ed in"
                            + " MaAbout Frank Zappa]}}",
                        "{likedate=20120904114300885, liker={firstName=[John], lastName=[Kumar],"
                            + " id=[32985348836287]}, message={id=[1786706617430], content=[About"
                            + " Mickey Rooney, iner whose film, telAbout Michael Moore, ions. He"
                            + " has als]}}",
                        "{likedate=20120902221041145, liker={firstName=[Ivan], lastName=[Santiago],"
                            + " id=[32985348841493]}, message={id=[1786707240991], content=[About"
                            + " Costa Rica, a to the southeast, the Pacific Ocean to theAbout"
                            + " Napoleon III, pula]}}",
                        "{likedate=20120902190352859, liker={firstName=[Baruch], lastName=[Dego],"
                            + " id=[4139]}, message={id=[2061584849006], content=[About Left"
                            + " Behind, game Left Behind: Eternal Forces and its sequels, Left ]}}",
                        "{likedate=20120902133650353, liker={firstName=[Hossein],"
                            + " lastName=[Karimi], id=[32985348842048]},"
                            + " message={id=[2061584849006], content=[About Left Behind, game Left"
                            + " Behind: Eternal Forces and its sequels, Left ]}}",
                        "{likedate=20120901170918823, liker={firstName=[Megumi], lastName=[Suzuki],"
                            + " id=[28587302326663]}, message={id=[1511830609010], content=[About"
                            + " Mariano Rivera, nt relievers in major league history. PAbout Naima,"
                            + " an ]}}",
                        "{likedate=20120901130728341, liker={firstName=[Deepak], lastName=[Bose],"
                            + " id=[24189255812226]}, message={id=[2061584849006], content=[About"
                            + " Left Behind, game Left Behind: Eternal Forces and its sequels, Left"
                            + " ]}}",
                        "{likedate=20120901113114136, liker={firstName=[Zeki], lastName=[Arikan],"
                            + " id=[19791209300402]}, message={id=[2061584849006], content=[About"
                            + " Left Behind, game Left Behind: Eternal Forces and its sequels, Left"
                            + " ]}}",
                        "{likedate=20120901095813249, liker={firstName=[Manuel], lastName=[Cosio],"
                            + " id=[28587302332758]}, message={id=[2061584849006], content=[About"
                            + " Left Behind, game Left Behind: Eternal Forces and its sequels, Left"
                            + " ]}}",
                        "{likedate=20120901072309081, liker={firstName=[Jacques],"
                            + " lastName=[Arnaud], id=[15393162795133]},"
                            + " message={id=[2061584849006], content=[About Left Behind, game Left"
                            + " Behind: Eternal Forces and its sequels, Left ]}}",
                        "{likedate=20120831225619539, liker={firstName=[Ching], lastName=[Hoang],"
                            + " id=[32985348833559]}, message={id=[1236951518889], content=[About"
                            + " Pope Paul VI, cese, yet denying hiAbout Bono, tress reduction. It"
                            + " About Lou Reed,  an American rock muAbout ]}}",
                        "{likedate=20120831154258335, liker={firstName=[Ahmad Rafiq],"
                            + " lastName=[Akbar], id=[17592186053137]},"
                            + " message={id=[2061584849006], content=[About Left Behind, game Left"
                            + " Behind: Eternal Forces and its sequels, Left ]}}",
                        "{likedate=20120831135801287, liker={firstName=[Ismail], lastName=[Aziz],"
                            + " id=[10995116282290]}, message={id=[2061584849006], content=[About"
                            + " Left Behind, game Left Behind: Eternal Forces and its sequels, Left"
                            + " ]}}",
                        "{likedate=20120831122429543, liker={firstName=[Ali], lastName=[Lo],"
                            + " id=[8796093029002]}, message={id=[2061584849006], content=[About"
                            + " Left Behind, game Left Behind: Eternal Forces and its sequels, Left"
                            + " ]}}");
        while (traversal.hasNext()) {
            Map<String, Object> bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected.get(counter)));
            ++counter;
        }

        Assert.assertEquals(expected.size(), (long) counter);
    }

    @Test
    public void run_ldbc_8_test() {
        Traversal<Vertex, Map<String, Object>> traversal = this.get_ldbc_8_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        Set<String> expected =
                Sets.newHashSet(
                        "{comment={id=[1786706501264], creationDate=[20120508164718581],"
                            + " content=[About Denmark, ual struggle for control of the Baltic Sea;"
                            + " before the digging of the Kiel]}, commenter={firstName=[Isabel],"
                            + " lastName=[Garcia], id=[10995116286316]}}",
                        "{comment={id=[1786706501272], creationDate=[20120417052950802],"
                                + " content=[About Jorge Luis Borges, ia. Scholars have also"
                                + " suggesteAbout Hong Kong, en described as E]},"
                                + " commenter={firstName=[Abhishek], lastName=[Rao],"
                                + " id=[8796093031885]}}",
                        "{comment={id=[1786711226851], creationDate=[20120421111140819],"
                            + " content=[About Thomas Edison,  and factories – About Buddy Holly, d"
                            + " a pioneer of roAbout Samu]}, commenter={firstName=[Arif],"
                            + " lastName=[Lesmana], id=[5976]}}",
                        "{comment={id=[1786711226855], creationDate=[20120421005529694],"
                                + " content=[About John Rhys-Davies, ohn Rhys-Davies (About Dog Man"
                                + " Star, it is considered Ab]}, commenter={firstName=[Jan],"
                                + " lastName=[Anton], id=[10995116288583]}}",
                        "{comment={id=[1924145454735], creationDate=[20120509045720143],"
                            + " content=[cool]}, commenter={firstName=[Deepak], lastName=[Kumar],"
                            + " id=[4398046513018]}}",
                        "{comment={id=[1374389640939], creationDate=[20111019210610461],"
                            + " content=[duh]}, commenter={firstName=[Abhishek], lastName=[Rao],"
                            + " id=[8796093031885]}}",
                        "{comment={id=[1786711226853], creationDate=[20120421210719836],"
                            + " content=[roflol]}, commenter={firstName=[Eddie], lastName=[Garcia],"
                            + " id=[5330]}}",
                        "{comment={id=[1374389640931], creationDate=[20111019163439495],"
                                + " content=[no way!]}, commenter={firstName=[Abhishek],"
                                + " lastName=[Rao], id=[8796093031885]}}",
                        "{comment={id=[1786711226854], creationDate=[20120421030229805],"
                            + " content=[About Samuel Taylor Coleridge, ic, literaryAbout Thomas"
                            + " Edison, dern industrA]}, commenter={firstName=[Mads],"
                            + " lastName=[Haugland], id=[13194139543018]}}",
                        "{comment={id=[2061584551353], creationDate=[20120902192046400],"
                                + " content=[ok]}, commenter={firstName=[John], lastName=[Iyar],"
                                + " id=[28587302328223]}}",
                        "{comment={id=[1786706501274], creationDate=[20120416184458353],"
                                + " content=[duh]}, commenter={firstName=[John], lastName=[Wilson],"
                                + " id=[1490]}}",
                        "{comment={id=[1786711226865], creationDate=[20120421221926988],"
                            + " content=[About Euripides, ives that wereAbout John Rhys-Davies, d"
                            + " the voices oAbout Henry D]}, commenter={firstName=[Antonio],"
                            + " lastName=[Garcia], id=[13194139539603]}}",
                        "{comment={id=[2061584551352], creationDate=[20120903111223692],"
                            + " content=[maybe]}, commenter={firstName=[Ashin], lastName=[Karat],"
                            + " id=[17592186044810]}}",
                        "{comment={id=[1786706501276], creationDate=[20120417004630904],"
                                + " content=[About Felix Mendelssohn,  Franz Liszt, RichaAbout Tony"
                                + " Blair, our Prime Mi]}, commenter={firstName=[Deepak],"
                                + " lastName=[Kumar], id=[4398046513018]}}",
                        "{comment={id=[1374389640938], creationDate=[20111019193909069],"
                            + " content=[cool]}, commenter={firstName=[Albert], lastName=[Bolier],"
                            + " id=[8796093031407]}}",
                        "{comment={id=[1374389640946], creationDate=[20111020022520841],"
                            + " content=[ok]}, commenter={firstName=[Isabel], lastName=[Garcia],"
                            + " id=[10995116286316]}}",
                        "{comment={id=[2061584551354], creationDate=[20120902192731040],"
                                + " content=[About Lil Wayne, m. Lil Wayne released his debut rock"
                                + " album, Rebirth, in 2]}, commenter={firstName=[Farrukh],"
                                + " lastName=[Znaimer], id=[17592186048413]}}",
                        "{comment={id=[1511830965950], creationDate=[20111221171052493], content=[I"
                                + " see]}, commenter={firstName=[Rahul], lastName=[Reddy],"
                                + " id=[19791209303129]}}",
                        "{comment={id=[1511828737702], creationDate=[20111206230226884],"
                            + " content=[thanks]}, commenter={firstName=[K.], lastName=[Sharma],"
                            + " id=[9773]}}",
                        "{comment={id=[1374389640934], creationDate=[20111019225544527],"
                                + " content=[About Desiderius Erasmus, ined committed to reforming"
                                + " theAbout George Washington,  his wea]},"
                                + " commenter={firstName=[Yang], lastName=[Zhang],"
                                + " id=[13194139535025]}}");

        while (traversal.hasNext()) {
            Map<String, Object> bindings = traversal.next();
            Assert.assertTrue(expected.contains(bindings.toString()));
            ++counter;
        }

        Assert.assertEquals(expected.size(), (long) counter);
    }

    @Test
    public void run_ldbc_9_test() {
        Traversal<Vertex, Map<String, Object>> traversal = this.get_ldbc_9_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        List<String> expected =
                Arrays.asList(
                        "{post={id=[1511829711860], creationDate=[20111216235809425],"
                                + " content=[About Augustine of Hippo, osopher and theologian from"
                                + " Roman Africa. About Che Gueva]}, friends={lastName=[Wang],"
                                + " firstName=[Xiaolu], id=[2199023260919]}}",
                        "{post={id=[1511830666887], creationDate=[20111216235709064],"
                            + " content=[good]}, friends={lastName=[Yamada], firstName=[Prince],"
                            + " id=[2199023260291]}}",
                        "{post={imageFile=[], id=[1511831473649], creationDate=[20111216235526728],"
                            + " content=[About Ho Chi Minh, ource Consulting, Economist"
                            + " Intelligence Unit and ECA International, Ho Chi Minh City is ranked"
                            + " 132 on the list of]}, friends={lastName=[Brown], firstName=[Jack],"
                            + " id=[10995116288703]}}",
                        "{post={id=[1511834999905], creationDate=[20111216235442124],"
                            + " content=[yes]}, friends={lastName=[Hosseini], firstName=[Hossein],"
                            + " id=[19791209310913]}}",
                        "{post={id=[1511828923913], creationDate=[20111216235355410],"
                                + " content=[ok]}, friends={lastName=[Khan], firstName=[Kiran],"
                                + " id=[8796093029365]}}",
                        "{post={id=[1511829478450], creationDate=[20111216235154542],"
                                + " content=[dia.org American Samoa competed at the 2004 Summer"
                                + " Olympics in Athens, Greece.About Ame]},"
                                + " friends={lastName=[Amenábar], firstName=[Carlos],"
                                + " id=[10995116279387]}}",
                        "{post={id=[1511829728931], creationDate=[20111216234926822],"
                                + " content=[cool]}, friends={lastName=[Khan], firstName=[Babar],"
                                + " id=[8796093030398]}}",
                        "{post={id=[1511828864535], creationDate=[20111216234741888],"
                            + " content=[LOL]}, friends={lastName=[Codreanu], firstName=[Victor],"
                            + " id=[17592186047200]}}",
                        "{post={imageFile=[], id=[1511829728929], creationDate=[20111216234716228],"
                                + " content=[About Adolf Hitler, is views were more or less formed"
                                + " during three perioAbout Aristotle, odern advent ]},"
                                + " friends={lastName=[Kazadi], firstName=[Jean van de],"
                                + " id=[8796093026337]}}",
                        "{post={id=[1511829863227], creationDate=[20111216234401470],"
                                + " content=[ok]}, friends={lastName=[David], firstName=[Mihai],"
                                + " id=[17592186045238]}}",
                        "{post={id=[1511828832823], creationDate=[20111216234140954],"
                                + " content=[About Henry Kissinger, ecretary of State in the"
                                + " administratioAbout Luxembo]}, friends={lastName=[Donati],"
                                + " firstName=[Giuseppe], id=[8796093028051]}}",
                        "{post={id=[1511831505173], creationDate=[20111216233920753], content=[no"
                                + " way!]}, friends={lastName=[Chen], firstName=[Lin], id=[9850]}}",
                        "{post={id=[1511829861734], creationDate=[20111216233654529],"
                                + " content=[thx]}, friends={lastName=[Redl], firstName=[Eva],"
                                + " id=[21990232557420]}}",
                        "{post={id=[1511831310812], creationDate=[20111216233627372],"
                            + " content=[About Jackson Browne, notable songs throughout About Rudy"
                            + " Giuliani, t to run and remain activAbout Jefferson Davis,  the]},"
                            + " friends={lastName=[Xu], firstName=[Anson], id=[17592186049298]}}",
                        "{post={id=[1511835042602], creationDate=[20111216233450452], content=[no"
                                + " way!]}, friends={lastName=[Abouba], firstName=[Hamani],"
                                + " id=[15393162791608]}}",
                        "{post={imageFile=[photo1511833666259.jpg], id=[1511833666259],"
                                + " creationDate=[20111216233439099], content=[]},"
                                + " friends={lastName=[Yamada], firstName=[Prince],"
                                + " id=[2199023260291]}}",
                        "{post={imageFile=[photo1511833666258.jpg], id=[1511833666258],"
                                + " creationDate=[20111216233438099], content=[]},"
                                + " friends={lastName=[Yamada], firstName=[Prince],"
                                + " id=[2199023260291]}}",
                        "{post={imageFile=[photo1511833666257.jpg], id=[1511833666257],"
                                + " creationDate=[20111216233437099], content=[]},"
                                + " friends={lastName=[Yamada], firstName=[Prince],"
                                + " id=[2199023260291]}}",
                        "{post={imageFile=[photo1511833666256.jpg], id=[1511833666256],"
                                + " creationDate=[20111216233436099], content=[]},"
                                + " friends={lastName=[Yamada], firstName=[Prince],"
                                + " id=[2199023260291]}}",
                        "{post={imageFile=[photo1511833666255.jpg], id=[1511833666255],"
                                + " creationDate=[20111216233435099], content=[]},"
                                + " friends={lastName=[Yamada], firstName=[Prince],"
                                + " id=[2199023260291]}}");

        while (traversal.hasNext()) {
            Map<String, Object> bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected.get(counter)));
            ++counter;
        }

        Assert.assertEquals(expected.size(), (long) counter);
    }

    @Test
    public void run_ldbc_11_test() {
        Traversal<Vertex, Map<String, Object>> traversal = this.get_ldbc_11_test();
        this.printTraversalForm(traversal);
        int counter = 0;

        List<String> expected =
                Arrays.asList(
                        "{works=2002, orgname=Lao_Airlines, friends={lastName=[Pham],"
                                + " firstName=[Eve-Mary Thai], id=[6597069767125]}}",
                        "{works=2002, orgname=Lao_Airlines, friends={lastName=[Hafez],"
                                + " firstName=[Atef], id=[28587302330691]}}",
                        "{works=2004, orgname=Lao_Airlines, friends={lastName=[Vorachith],"
                                + " firstName=[Cy], id=[5869]}}",
                        "{works=2005, orgname=Lao_Air, friends={lastName=[Vang], firstName=[Mee],"
                                + " id=[8796093022909]}}",
                        "{works=2005, orgname=Lao_Airlines, friends={lastName=[Charoenpura],"
                                + " firstName=[Jetsada], id=[10995116285549]}}",
                        "{works=2006, orgname=Lao_Airlines, friends={lastName=[Anwar],"
                                + " firstName=[A.], id=[24189255815555]}}",
                        "{works=2007, orgname=Lao_Air, friends={lastName=[Li], firstName=[Ben],"
                                + " id=[2199023266276]}}",
                        "{works=2007, orgname=Lao_Airlines, friends={lastName=[Sysavanh],"
                                + " firstName=[Pao], id=[8796093027636]}}",
                        "{works=2008, orgname=Lao_Air, friends={lastName=[Vongvichit],"
                                + " firstName=[Mee], id=[1259]}}",
                        "{works=2009, orgname=Lao_Air, friends={lastName=[Achiou], firstName=[Ali],"
                                + " id=[2199023258003]}}");

        while (traversal.hasNext()) {
            Map<String, Object> bindings = traversal.next();
            Assert.assertTrue(bindings.toString().equals(expected.get(counter)));
            ++counter;
        }

        Assert.assertEquals(expected.size(), (long) counter);
    }

    @Test
    public void run_ldbc_12_test() {
        Traversal<Vertex, Map<Object, Long>> traversal = this.get_ldbc_12_test();
        this.printTraversalForm(traversal);

        String expected =
                "{v[72061992084440954]=5, v[72066390130957203]=5, v[72070788177462961]=5,"
                        + " v[72057594037929426]=4, v[72057594037935661]=4, v[72066390130959821]=4,"
                        + " v[72066390130959343]=3, v[72068589154214252]=2, v[72068589154207326]=1,"
                        + " v[72081783293739876]=1}";
        Map<Object, Long> result = traversal.next();
        Assert.assertEquals(expected, result.toString());
        Assert.assertFalse(traversal.hasNext());
    }

    public static class Traversals extends LdbcQueryTest {

        @Override
        public Traversal<Vertex, Map<String, Object>> get_ldbc_1_test() {
            return ((IrCustomizedTraversal)
                            g.V().hasLabel("PERSON")
                                    .has("id", 30786325583618L)
                                    .both("1..4", "KNOWS")
                                    .as("p"))
                    .endV()
                    .has("id", P.neq(30786325583618L))
                    .has("firstName", P.eq("Chau"))
                    .as("a")
                    .order()
                    .by(__.select("p").by("~len"), Order.asc)
                    .dedup()
                    .order()
                    .by(__.select("p").by("~len"), Order.asc)
                    .by(__.select("a").by("id"))
                    .by(__.select("a").by("lastName"))
                    .limit(20)
                    .select("a", "p")
                    .by(__.valueMap("id", "firstName", "lastName"))
                    .by("~len");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_ldbc_2_test() {
            return g.V().hasLabel("PERSON")
                    .has("id", 17592186044810L)
                    .both("KNOWS")
                    .as("p")
                    .in("HASCREATOR")
                    .has("creationDate", P.lte(20120803072025654L))
                    .order()
                    .by("creationDate", Order.desc)
                    .by("id", Order.asc)
                    .limit(20)
                    .as("m")
                    .select("p", "m")
                    .by(__.valueMap("id", "firstName", "lastName"))
                    .by(__.valueMap("id", "imageFile", "creationDate", "content"));
        }

        @Override
        public Traversal<Vertex, Vertex> get_ldbc_3_test() {
            return g.V().hasLabel("PERSON")
                    .has("id", 17592186055119L)
                    .union(__.both("KNOWS"), __.both("KNOWS").both("KNOWS"))
                    .dedup()
                    .where(__.out("ISLOCATEDIN").has("name", P.without("Laos", "United_States")))
                    .where(
                            __.in("HASCREATOR")
                                    .has(
                                            "creationDate",
                                            P.gt(20110601000000000L).and(P.lt(20110713000000000L)))
                                    .out("ISLOCATEDIN")
                                    .has("name", P.eq("Laos").or(P.eq("Scotland")))
                                    .values("name")
                                    .dedup()
                                    .count()
                                    .is(2))
                    .order()
                    .by(
                            __.in("HASCREATOR")
                                    .has(
                                            "creationDate",
                                            P.gt(20110601000000000L).and(P.lt(20110713000000000L)))
                                    .out("ISLOCATEDIN")
                                    .has("name", "Laos")
                                    .count(),
                            Order.desc)
                    .by("id", Order.asc)
                    .limit(20);
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_ldbc_4_test() {
            return g.V().hasLabel("PERSON")
                    .has("id", 15393162790846L)
                    .as("person")
                    .both("KNOWS")
                    .in("HASCREATOR")
                    .hasLabel("POST")
                    .as("post")
                    .has("creationDate", P.gte(20120801000000000L).and(P.lt(20120830000000000L)))
                    .out("HASTAG")
                    .as("tag")
                    .select("person")
                    .not(
                            __.both("KNOWS")
                                    .in("HASCREATOR")
                                    .hasLabel("POST")
                                    .has("creationDate", P.lt(20120801000000000L))
                                    .out("HASTAG")
                                    .where(P.eq("tag")))
                    .select("tag")
                    .groupCount()
                    .order()
                    .by(__.select(Column.values), Order.desc)
                    .by(__.select(Column.keys).values("name"), Order.asc)
                    .limit(10)
                    .select(Column.keys)
                    .values("name")
                    .as("tagName")
                    .select(Column.values)
                    .as("postCount")
                    .select("tagName", "postCount");
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_ldbc_5_test() {
            return ((IrCustomizedTraversal)
                            g.V().hasLabel("PERSON")
                                    .has("id", 21990232560302L)
                                    .both("1..3", "KNOWS"))
                    .endV()
                    .dedup()
                    .as("p")
                    .inE("HASMEMBER")
                    .has("joinDate", P.gt(20120901000000000L))
                    .outV()
                    .as("forum")
                    .out("CONTAINEROF")
                    .hasLabel("POST")
                    .out("HASCREATOR")
                    .where(P.eq("p"))
                    .select("forum")
                    .groupCount()
                    .order()
                    .by(__.select(Column.values), Order.desc)
                    .by(__.select(Column.keys).values("id"), Order.asc)
                    .limit(20);
        }

        

        @Override
        public Traversal<Vertex, Map<String, Object>> get_ldbc_6_test() {
            return g.V().hasId(72088380363511554L)
                    .union(__.both("KNOWS"), __.both("KNOWS").both("KNOWS"))
                    .dedup()
                    .has("id", P.neq(30786325583618L))
                    .in("HASCREATOR")
                    .hasLabel("POST")
                    .as("_t")
                    .out("HASTAG")
                    .has("name", P.eq("Angola"))
                    .select("_t")
                    .dedup()
                    .out("HASTAG")
                    .has("name", P.neq("Angola"))
                    .groupCount()
                    .order()
                    .by(__.select(Column.values), Order.desc)
                    .by(__.select(Column.keys).values("name"), Order.asc)
                    .limit(10)
                    .select(Column.keys)
                    .values("name")
                    .as("keys")
                    .select(Column.values)
                    .as("values")
                    .select("keys", "values");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_ldbc_7_test() {
            return g.V().hasLabel("PERSON")
                    .has("id", 17592186053137L)
                    .in("HASCREATOR")
                    .as("message")
                    .inE("LIKES")
                    .as("like")
                    .values("creationDate")
                    .as("likedate")
                    .select("like")
                    .outV()
                    .as("liker")
                    .order()
                    .by(__.select("likedate"), Order.desc)
                    .by("id", Order.asc)
                    .limit(20)
                    .select("message", "likedate", "liker")
                    .by(__.valueMap("id", "content", "imageFile"))
                    .by()
                    .by(__.valueMap("id", "firstName", "lastName"));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_ldbc_8_test() {
            return g.V().hasLabel("PERSON")
                    .has("id", 17592186044810L)
                    .in("HASCREATOR")
                    .in("REPLYOF")
                    .hasLabel("COMMENT")
                    .as("comment")
                    .order()
                    .by("creationDate", Order.desc)
                    .by("id", Order.asc)
                    .limit(20)
                    .out("HASCREATOR")
                    .as("commenter")
                    .select("commenter", "comment")
                    .by(__.valueMap("id", "firstName", "lastName"))
                    .by(__.valueMap("creationDate", "id", "content"));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_ldbc_9_test() {
            return ((IrCustomizedTraversal)
                            g.V().hasLabel("PERSON")
                                    .has("id", 13194139542834L)
                                    .both("1..3", "KNOWS"))
                    .endV()
                    .dedup()
                    .has("id", P.neq(13194139542834L))
                    .as("friends")
                    .in("HASCREATOR")
                    .has("creationDate", P.lt(20111217000000000L))
                    .as("post")
                    .order()
                    .by("creationDate", Order.desc)
                    .by("id", Order.asc)
                    .limit(20)
                    .select("friends", "post")
                    .by(__.valueMap("id", "firstName", "lastName"))
                    .by(__.valueMap("id", "content", "imageFile", "creationDate"));
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_ldbc_11_test() {
            return ((IrCustomizedTraversal)
                            g.V().hasLabel("PERSON")
                                    .has("id", 30786325583618L)
                                    .as("root")
                                    .both("1..3", "KNOWS"))
                    .endV()
                    .dedup()
                    .has("id", P.neq(30786325583618L))
                    .as("friends")
                    .outE("WORKAT")
                    .has("workFrom", P.lt(2010))
                    .as("startWork")
                    .values("workFrom")
                    .as("works")
                    .select("startWork")
                    .inV()
                    .as("comp")
                    .values("name")
                    .as("orgname")
                    .select("comp")
                    .out("ISLOCATEDIN")
                    .has("name", "Laos")
                    .select("friends")
                    .order()
                    .by(__.select("works"), Order.asc)
                    .by("id", Order.asc)
                    .by(__.select("orgname"), Order.desc)
                    .limit(10)
                    .select("friends", "orgname", "works")
                    .by(__.valueMap("id", "firstName", "lastName"))
                    .by()
                    .by();
        }

        @Override
        public Traversal<Vertex, Map<Object, Long>> get_ldbc_12_test() {
            return g.V().hasLabel("PERSON")
                    .has("id", 17592186044810L)
                    .both("KNOWS")
                    .as("friend")
                    .in("HASCREATOR")
                    .hasLabel("COMMENT")
                    .where(
                            __.out("REPLYOF")
                                    .hasLabel("POST")
                                    .out("HASTAG")
                                    .out("HASTYPE")
                                    .has("name", P.eq("BasketballPlayer")))
                    .select("friend")
                    .groupCount()
                    .order()
                    .by(__.select(Column.values), Order.desc)
                    .by(__.select(Column.keys).values("id"), Order.asc)
                    .limit(20);
        }
    }
}
