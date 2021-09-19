# Pegasus
Pegasus is a distributed data-parallel compute engine based on the **cyclic** dataflow computation
model. Pegasus serves as the computation engine, lying at the core of the [GraphScope](https://github.com/alibaba/GraphScope) system.
Users can construct the computation via a directed acyclic graph (DAG), and easily run the job
on their laptop or even a distributed environment across a cluster of computers. Note that cycle
can be introduced via the loop control flow, while the whole loop contexts will be wrapped in a
**scope** within which the cycle is completely hidden from the users. **Scope** is a unique
concept of Pegasus to handle complex control flow such as loop and conditional. In addition, it is
also the key to many application-level primitives, e.g. correlated subtask, as well as
advanced scheduling techniques, e.g. early-stop mechanism. The concept of **scope** and many of
its salient use cases have been discussed in this research [paper](https://www.usenix.org/biblio-6260).


# A Toy Example
You can easily write Pegasus dataflow programs like the following one:

```Rust
extern crate pegasus;

use pegasus::api::*;
use pegasus::{Configuration, JobConf};

fn main() {
    pegasus_common::logs::init_log();
    // Note that the actual configuration is different in pegasus/examples/word_count_toy.rs
    pegasus::startup(Configuration::singleton()).ok();
    let mut conf = JobConf::new("word count");
    let mut result = pegasus::run(conf, || {
        let id = pegasus::get_current_worker().index;
        move |input, output| {
            let lines = if id == 0 {
                // Feed the input data
                vec!["This is a simple test".to_string(), "This is a wonderful world".to_string()].into_iter()
            } else {
                vec![].into_iter()
            };
            input
                .input_from(lines)?
                // Break line into words by space.
                .flat_map(|line| {
                    let words = line
                        .split(' ')
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>();
                    Ok(words.into_iter())
                })?
                // Initialize each word with count 1
                .map(|word| Ok((word, 1)))?
                // Make key-value pair from the pair
                .keyed()?
                // Reduce the count for each key, namely word
                .reduce_by_key(|| |a, b| Ok(a + b))?
                // Collect the results into one machine
                .unfold(|map| Ok(map.into_iter()))?
                .collect::<Vec<_>>()?
                // Sink to the results
                .sink_into(output)
        }
    })
    .expect("run job failure;");

    let mut result = result.next().unwrap().unwrap();
    result.sort_by(|a, b| b.1.cmp(&a.1));
    for (word, count) in result {
        println!("word: {}, count: {}", word, count);
    }
    pegasus::shutdown_all();
}
```

Note that the codes are also available in "pegasus/examples/word_count_toy.rs". Before Pegasus is
published to crates.io, "pegasus/examples" is the recommended directory to place the user codes.

You can run the above example from the "pegasus" directory of the repository by typing:
```bash
Cargo run --example word_count_toy
Running `target/debug/examples/word_count_toy`
word: is, count: 2
word: a, count: 2
word: This, count: 2
word: world, count: 1
word: simple, count: 1
word: test, count: 1
word: wonderful, count: 1
```

In your laptop, one can easily run the above codes in parallel using two workers via `-w` option, as:
```bash
Cargo run --example word_count_toy -- -w 2
```

# Running In Distribution
Pegasus is a distributed system that allows easily running your program on a cluster of potentially
many hosts (can be physical machines or virtual containers). To do so, users need to provide a file
that contains all hosts and their logical indices (from 0 to #hosts - 1). Suppose there are two
hosts, 192.168.0.1 and 192.168.0.2, one may specify the hosts file "hosts.toml" as:
```toml
[[peers]]
server_id = 0
ip = '192.168.0.1'
port = 1234
[[peers]]
server_id = 1
ip = '192.168.0.2'
port = 1234
```

Here, server_id refers to the logical index of the given host, and port specifies the port for
network communication. Suppose the "hosts.toml" has been placed in the path of `/path/to/hosts.toml`,
Pegasus provides the following codes to startup the engine:
```Rust
let server_conf = Configuration::parse(
    std::fs::read_to_string("/path/to/hosts.toml").unwrap()
).unwrap();
pegasus::startup(server_conf).ok();
```

Note that the above codes only configure the hosts for communication. While submitting jobs, one
can also specify the degree of parallelism (#workers) on each host, via:
```Rust
let mut conf = JobConf::new("job_name");
conf.set_workers(#workers);
```

For ease of testing, one can simulate the distributed runtime by specifying localhost as the
ip addresses for each host, but with different port number, as:
```toml
[[peers]]
server_id = 0
ip = '127.0.0.1'
port = 1234
[[peers]]
server_id = 1
ip = '127.0.0.1'
port = 1235
```

To run the "word_count_toy" example in the distributed context, one simply goes into "pegasus"
under the root directory of the repository and then compiles the codes on one of the machine via:
```
cargo build --release --example word_count_toy
```

The compiling may take a while. After it is done, a binary file "word_count_toy" can be found
under the directory of "target/release/examples" (Note that this is under the root directory).
Copy the binary file and the hosts file to the same folder, saying "/path/to/work_dir" on each host,
and then running the following codes on all hosts simultaneously (can do remote execution via `ssh`):
```bash
host0% /path/to/work_dir/word_count_toy -s /path/to/work_dir/hosts.toml -w <#workers>
host1% /path/to/work_dir/word_count_toy -s /path/to/work_dir/hosts.toml -w <#workers>
```

# Cyclic Dataflow
A powerful feature of pegasus is cyclic dataflow, namely, it allows users to
easily program loop control flow that can repeat the execution of a sub-dataflow until arriving
at a fixed point. To support cyclic dataflow, Pegasus introduces the api of `iterate()`.

Let's see a ping-pong example (pegasus/examples/ping_long.rs), where two workers are hitting
the "ball" back and forth to simulate a ping-pong game. Assume there is a ball serving from
some player, which is of the tuple type `(u32, u32)`, with the first field indicating which
player it is (0 and 1) and the second field pointing to the ball power (within 0..30).
In each point, the players are hitting the ball back and forth until someone has produced a 0 ball
power. To simulate the process, we write the following dataflow codes:
```Rust
fn single_play(serving: Stream<(u32, u32)>) -> Result<Stream<(u32, u32)>, BuildJobError> {
    const LOSS: u32 = 0;
    // Specify the maximum iterations as 20 to avoid running a long loop
    let mut until = LoopCondition::<(u32, u32)>::max_iters(20);
    // Specify the termination is anyone hits a zero ball power
    until.until_fn(move |(_player, ball)| Ok(*ball == LOSS));

    // Suppose serving is fed as a data stream of data `(u32, u32)`.
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
```

Whoever hits a `LOSS` ball in the above point play will let the other player wins one point.
A set of ping-pong game will loop against the above point plays, until whoever get more than 11
points wins by at least two points. With this setting, we shall exemplify another salient
feature of Pegasus -- loop nesting, a very challenging control flow in the distributed context.

We define the following data structure:
```Rust
#[derive(Default, Debug, Clone)]
struct PlayerScore {
    /// Play 0 or 1
    player: u32,
    /// The score that the current play has won
    points: u32,
}

/// Implement `Add` to allow using the "+" operator
impl Add for PlayerScore {
    type Output = PlayerScore;

    fn add(self, other: Self) -> Self::Output {
        assert_eq!(self.player, other.player);
        Self {
            player: self.player,
            points: self.points + other.points,
        }
    }
}
```

The inputs to the game are the initial `PlayerScore` pf each player as:
```Rust
let points = if id == 0 {
    input.input_from(vec![(PlayerScore::new(0, 0), PlayerScore::new(1, 0))].into_iter())
} else {
    input.input_from(vec![].into_iter())
}?;
```

We shall define the termination condition of the game, namely, he who get more than 11 points
wins by at least two points.

```Rust
let mut until = LoopCondition::<(PlayerScore, PlayerScore)>::max_iters(100);
until.until(move |(ps1, ps2)| {
    Ok((ps1.points as i32 - ps2.points as i32).abs() >= 2
        && (ps1.points >= 11 || ps2.points >= 11))
});
```

Then, we feed the `PlayerScore` of both players into a loop control flow, within which
the `single_play` will present as a nested loop.

```Rust
points
    .iterate_until(until, |start| {
        let (points1, points2) = start.copied()?;
        let serving = points1.map(|(ps1, ps2)| {
        let total_points = ps1.points + ps2.points;
            if total_points % 4 < 2 {
                // player 0 serves
                Ok((0, 30_u32))
            } else {
                // player 1 serves
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
            // Merge the current point to total points
            .merge(points2)?
            .reduce(|| |pair1, pair2| Ok((pair1.0 + pair2.0, pair1.1 + pair2.1)))?
            .into_stream()
```
