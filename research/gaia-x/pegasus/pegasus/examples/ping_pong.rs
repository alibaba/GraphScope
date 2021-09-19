extern crate pegasus;

use std::io;
use std::ops::Add;

use pegasus::api::{Collect, IterCondition, Iteration, Map, Merge, Reduce, Sink};
use pegasus::stream::Stream;
use pegasus::{BuildJobError, Configuration, JobConf};
use pegasus_common::codec::{Decode, Encode};
use pegasus_common::io::{ReadExt, WriteExt};
use rand::{thread_rng, Rng};

const LOSS: u32 = 0;

/// Each serving consists of a tuple, where tuple.0 indicates the player id,
/// and tuple.1 indicates the ball that it is serving. The game continues
/// until any player hits a LOSS ball, or it exceeds a random `max_iters`.
fn single_play(serving: Stream<(u32, u32)>) -> Result<Stream<(u32, u32)>, BuildJobError> {
    let max_iters = 30;
    let mut until = IterCondition::<(u32, u32)>::max_iters(max_iters);
    until.until(move |(_player, ball)| Ok(*ball == LOSS));

    serving.iterate_until(until, |start| {
        start
            // Hit the ball to the opponent side, aka, 0 -> 1, 1 -> 0
            .repartition(|(player, _ball)| Ok((*player ^ 1) as u64))
            .map(|(player, ball)| {
                // The larger ball is, the easier it is to hit the ball back, which means
                // the less possible for the other player to loss (hit a zero number)
                let new_ball = thread_rng().gen_range(LOSS..ball);
                println!("Player {:?} hits a new ball {:?}", player ^ 1, new_ball);
                Ok((player ^ 1, new_ball))
            })
    })
}

#[derive(Default, Debug, Eq, PartialEq, Ord, PartialOrd, Clone)]
struct PlayerScore {
    player: u32,
    points: u32,
}

impl PlayerScore {
    fn new(player: u32, points: u32) -> Self {
        Self { player, points }
    }
}

impl Add for PlayerScore {
    type Output = PlayerScore;

    fn add(self, other: Self) -> Self::Output {
        assert_eq!(self.player, other.player);
        Self { player: self.player, points: self.points + other.points }
    }
}

impl Encode for PlayerScore {
    fn write_to<W: WriteExt>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u32(self.player)?;
        writer.write_u32(self.points)
    }
}

impl Decode for PlayerScore {
    fn read_from<R: ReadExt>(reader: &mut R) -> io::Result<Self> {
        let player = reader.read_u32()?;
        let points = reader.read_u32()?;
        Ok(PlayerScore { player, points })
    }
}

fn main() {
    pegasus_common::logs::init_log();
    pegasus::startup(Configuration::singleton()).ok();

    let mut conf = JobConf::new("ping_pong");
    conf.plan_print = true;
    conf.set_workers(2);

    let mut result = pegasus::run(conf, || {
        let id = pegasus::get_current_worker().index;
        move |input, output| {
            let points = if id == 0 {
                input.input_from(vec![(PlayerScore::new(0, 0), PlayerScore::new(1, 0))].into_iter())
            } else {
                input.input_from(vec![].into_iter())
            }?;

            let mut until = IterCondition::<(PlayerScore, PlayerScore)>::max_iters(100);
            until.until(move |(ps1, ps2)| {
                Ok((ps1.points as i32 - ps2.points as i32).abs() >= 2
                    && (ps1.points >= 11 || ps2.points >= 11))
            });

            points
                .iterate_until(until, |start| {
                    let (points1, points2) = start.copied()?;
                    let serving = points1.map(|(ps1, ps2)| {
                        let total_points = ps1.points + ps2.points;
                        if total_points % 4 < 2 {
                            Ok((0, 30_u32))
                        } else {
                            Ok((1, 30_u32))
                        }
                    })?;

                    single_play(serving)?
                        .map(|(player, ball)| {
                            // Determine how to add the points to each player
                            let (ps1, ps2) = if ball == LOSS {
                                // The other player gets one point
                                println!("Player {:?} wins the point", player ^ 1);
                                let ps1 = PlayerScore::new(player ^ 1, 1);
                                let ps2 = PlayerScore::new(player, 0);
                                (ps1, ps2)
                            } else {
                                // This player gets one points
                                println!("Player {:?} wins the point", player);
                                let ps1 = PlayerScore::new(player, 1);
                                let ps2 = PlayerScore::new(player ^ 1, 0);
                                (ps1, ps2)
                            };
                            if ps1.player == 0 {
                                Ok((ps1, ps2))
                            } else {
                                Ok((ps2, ps1))
                            }
                        })?
                        .merge(points2)?
                        .reduce(|| |pair1, pair2| Ok((pair1.0 + pair2.0, pair1.1 + pair2.1)))?
                        .into_stream()
                })?
                .collect::<Vec<(PlayerScore, PlayerScore)>>()?
                .sink_into(output)
        }
    })
    .expect("Build job error!");

    let final_points: Vec<(PlayerScore, PlayerScore)> = result.next().unwrap().unwrap();
    println!("{:?}", final_points);
}
