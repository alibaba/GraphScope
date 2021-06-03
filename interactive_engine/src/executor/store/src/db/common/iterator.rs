

pub struct IteratorList<T, I> where T: Iterator<Item=I> {
    iters: Vec<T>,
    curr_iter: Option<T>,
}

impl<T, I> IteratorList<T, I> where T: Iterator<Item=I> {
    pub fn new(iters: Vec<T>) -> Self {
        IteratorList {
            iters,
            curr_iter: None,
        }
    }
}

impl<T, I> Iterator for IteratorList<T, I> where T: Iterator<Item=I> {
    type Item = I;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        loop {
            if let Some(ref mut iter) = self.curr_iter {
                if let Some(x) = iter.next() {
                    return Some(x);
                } else {
                    if let Some(iter_val) = self.iters.pop() {
                        *iter = iter_val;
                    } else {
                        return None;
                    }
                }
            } else {
                if let Some(iter_val) = self.iters.pop() {
                    self.curr_iter = Some(iter_val);
                } else {
                    return None;
                }
            }
        }
    }
}