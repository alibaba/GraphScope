use crate::api::function::*;

#[macro_export]
macro_rules! throw_error {
    ($msg: expr) => {{
        let msg = format!("error : {} at: {}:{}", $msg, file!(), line!());
        let cause: Box<dyn std::error::Error + Send + Sync> = msg.into();
        Err(cause)
    }};
}

/// impl functions for closure;
/// Because of conflict implementation with functions impl for box, we compromised by using a
/// named structure to wrap the closure, and use macros for easy use;
///
#[macro_use]
pub mod route {
    use super::*;
    use crate::data::MicroBatch;
    use crate::Data;

    pub struct RouteClosure<D, F: Fn(&D) -> FnResult<u64>> {
        func: F,
        _ph: std::marker::PhantomData<D>,
    }

    impl<D, F: Fn(&D) -> FnResult<u64>> RouteClosure<D, F> {
        pub fn new(func: F) -> Self {
            RouteClosure { func, _ph: std::marker::PhantomData }
        }
    }

    impl<F, D> RouteFunction<D> for RouteClosure<D, F>
    where
        D: Send + 'static,
        F: Fn(&D) -> FnResult<u64> + Send + 'static,
    {
        fn route(&self, data: &D) -> FnResult<u64> {
            (self.func)(data)
        }
    }

    #[macro_export]
    macro_rules! route {
        ($func: expr) => {
            RouteClosure::new($func)
        };
    }

    #[macro_export]
    macro_rules! box_route {
        ($func: expr) => {
            Box::new(RouteClosure::new($func))
        };
    }

    pub struct BatchRouteClosure<D: Data, F: Fn(&MicroBatch<D>) -> FnResult<u64>> {
        func: F,
        _ph: std::marker::PhantomData<D>,
    }

    impl<D: Data, F: Fn(&MicroBatch<D>) -> FnResult<u64>> BatchRouteClosure<D, F> {
        pub fn new(func: F) -> Self {
            BatchRouteClosure { func, _ph: std::marker::PhantomData }
        }
    }

    impl<F, D> BatchRouteFunction<D> for BatchRouteClosure<D, F>
    where
        D: Data,
        F: Fn(&MicroBatch<D>) -> FnResult<u64> + Send + 'static,
    {
        fn route(&self, batch: &MicroBatch<D>) -> FnResult<u64> {
            (self.func)(batch)
        }
    }

    #[macro_export]
    macro_rules! batch_route {
        ($func: expr) => {
            BatchRouteClosure::new($func)
        };
    }

    #[macro_export]
    macro_rules! box_batch_route {
        ($func: expr) => {
            Box::new(BatchRouteClosure::new($func))
        };
    }
}
// pub struct MultiRouteClosure<D, F: Fn(&D) -> &[u64]> {
//     func: F,
//     _ph: std::marker::PhantomData<D>,
// }
//
// impl<D, F> MultiRouteClosure<D, F>
// where
//     F: Fn(&D) -> &[u64],
// {
//     pub fn new(func: F) -> Self {
//         MultiRouteClosure { func, _ph: std::marker::PhantomData }
//     }
// }
//
// impl<D, F> MultiRouteFunction<D> for MultiRouteClosure<D, F>
// where
//     D: Send + 'static,
//     F: Fn(&D) -> &[u64] + Send + 'static,
// {
//     fn route(&self, data: &D) -> &[u64] {
//         (self.func)(data)
//     }
// }
//
// #[macro_export]
// macro_rules! multi_route {
//     ($func: expr) => {
//         MultiRouteClosure::new($func)
//     };
// }

#[macro_use]
pub mod map {
    pub use crate::api::function::{DynError, FlatMapFunction, FnResult, MapFunction};

    pub struct MapClosure<I, O, F: Fn(I) -> FnResult<O>> {
        func: F,
        _ph: std::marker::PhantomData<(I, O)>,
    }

    impl<I, O, F: Fn(I) -> FnResult<O>> MapClosure<I, O, F> {
        pub fn new(func: F) -> Self {
            MapClosure { func, _ph: std::marker::PhantomData }
        }
    }

    impl<I, O, F> MapFunction<I, O> for MapClosure<I, O, F>
    where
        I: Send + 'static,
        O: Send + 'static,
        F: Fn(I) -> FnResult<O> + Send + 'static,
    {
        fn exec(&self, input: I) -> FnResult<O> {
            (self.func)(input)
        }
    }

    unsafe impl<I, O, F: Fn(I) -> FnResult<O> + Send + Sync + 'static> Sync for MapClosure<I, O, F> {}

    #[macro_export]
    macro_rules! map {
        ($func: expr) => {
            MapClosure::new($func)
        };
    }

    pub struct FlatMapClosure<I, O, F, R> {
        func: F,
        _ph: std::marker::PhantomData<(I, O, R)>,
    }

    impl<I, O, F, R> FlatMapClosure<I, O, F, R>
    where
        I: Send + 'static,
        O: Send + 'static,
        R: Iterator<Item = Result<O, DynError>> + Send + 'static,
        F: Fn(I) -> R + Send + 'static,
    {
        pub fn new(func: F) -> Self {
            FlatMapClosure { func, _ph: std::marker::PhantomData }
        }
    }

    impl<I, O, F, R> FlatMapFunction<I, O> for FlatMapClosure<I, O, F, R>
    where
        I: Send + 'static,
        O: Send + 'static,
        R: Iterator<Item = O> + Send + 'static,
        F: Fn(I) -> R + Send + 'static,
    {
        type Target = R;

        fn exec(&self, input: I) -> Result<Self::Target, DynError> {
            Ok((self.func)(input))
        }
    }

    #[macro_export]
    macro_rules! flat_map {
        ($func: expr) => {
            FlatMapClosure::new($func)
        };
    }
}
#[macro_use]
pub mod filter {
    use super::*;
    pub struct FilterClosure<D, F: Fn(&D) -> FnResult<bool>> {
        func: F,
        _ph: std::marker::PhantomData<D>,
    }

    impl<D, F> FilterClosure<D, F>
    where
        F: Fn(&D) -> FnResult<bool>,
    {
        pub fn new(func: F) -> Self {
            FilterClosure { func, _ph: std::marker::PhantomData }
        }
    }

    unsafe impl<D, F> Sync for FilterClosure<D, F> where F: Fn(&D) -> FnResult<bool> + Send + Sync {}

    impl<D, F> FilterFunction<D> for FilterClosure<D, F>
    where
        D: Send + 'static,
        F: Fn(&D) -> FnResult<bool> + Send + 'static,
    {
        fn test(&self, input: &D) -> FnResult<bool> {
            (self.func)(input)
        }
    }

    impl<D, F> MapFunction<D, bool> for FilterClosure<D, F>
    where
        D: Send + 'static,
        F: Fn(&D) -> FnResult<bool> + Send + 'static,
    {
        fn exec(&self, input: D) -> FnResult<bool> {
            self.test(&input)
        }
    }

    #[macro_export]
    macro_rules! filter {
        ($func: expr) => {
            FilterClosure::new($func)
        };
    }
}
//
// pub struct CompareClosure<D, F: Fn(&D, &D) -> Ordering + Send> {
//     func: F,
//     _ph: std::marker::PhantomData<D>,
// }
//
// impl<D: Send + 'static, F> CompareFunction<D> for CompareClosure<D, F>
// where
//     F: Fn(&D, &D) -> Ordering + Send + 'static,
// {
//     fn compare(&self, left: &D, right: &D) -> Ordering {
//         (self.func)(left, right)
//     }
// }
//
// impl<D, F> CompareClosure<D, F>
// where
//     F: Fn(&D, &D) -> Ordering + Send,
// {
//     pub fn new(func: F) -> Self {
//         CompareClosure { func, _ph: std::marker::PhantomData }
//     }
// }

// #[macro_export]
// macro_rules! compare {
//     ($func: expr) => {
//         CompareClosure::new($func)
//     };
// }
//
// pub struct EncodeClosure<D, F> {
//     func: F,
//     _ph: std::marker::PhantomData<D>,
// }
//
// impl<D, F> EncodeClosure<D, F>
// where
//     F: Fn(Vec<D>) -> Vec<u8>,
// {
//     pub fn new(func: F) -> Self {
//         EncodeClosure { func, _ph: std::marker::PhantomData }
//     }
// }
//
// impl<D, F> EncodeFunction<D> for EncodeClosure<D, F>
// where
//     D: Send + 'static,
//     F: Fn(Vec<D>) -> Vec<u8> + Send + 'static,
// {
//     fn encode(&self, data: Vec<D>) -> Vec<u8> {
//         (self.func)(data)
//     }
// }

// #[macro_export]
// macro_rules! encode {
//     ($func: expr) => {
//         EncodeClosure::new($func)
//     };
// }

#[macro_use]
pub mod factory {
    use pegasus_common::utils::Factory;

    pub struct FactoryFn<F, T> {
        func: F,
        _ph: std::marker::PhantomData<T>,
    }

    impl<F, T> FactoryFn<F, T> {
        pub fn new(func: F) -> Self {
            FactoryFn { func, _ph: std::marker::PhantomData }
        }
    }

    impl<F, T> Factory for FactoryFn<F, T>
    where
        F: Fn() -> T,
    {
        type Target = T;

        fn create(&self) -> Self::Target {
            (self.func)()
        }
    }

    #[macro_export]
    macro_rules! factory {
        ($func: expr) => {
            FactoryFn::new($func)
        };
    }
}

#[macro_use]
pub mod binary {
    use crate::api::function::{BinaryFunction, BinaryLeftMutFunction, FnResult};

    pub struct BinaryFn<F, L, R, O> {
        func: F,
        _ph: std::marker::PhantomData<(L, R, O)>,
    }

    impl<F, L, R, O> BinaryFn<F, L, R, O> {
        pub fn new(func: F) -> Self {
            BinaryFn { func, _ph: std::marker::PhantomData }
        }
    }

    impl<F, L, R, O> BinaryFunction<L, R, O> for BinaryFn<F, L, R, O>
    where
        L: Send + 'static,
        R: Send + 'static,
        O: Send + 'static,
        F: Fn(L, R) -> FnResult<O> + Send + 'static,
    {
        fn exec(&self, left: L, right: R) -> FnResult<O> {
            (self.func)(left, right)
        }
    }

    impl<F, L, R, O> BinaryLeftMutFunction<L, R, O> for BinaryFn<F, L, R, O>
    where
        L: Send + 'static,
        R: Send + 'static,
        O: Send + 'static,
        F: Fn(&mut L, R) -> FnResult<O> + Send + 'static,
    {
        fn exec(&self, left: &mut L, right: R) -> FnResult<O> {
            (self.func)(left, right)
        }
    }

    #[macro_export]
    macro_rules! binary {
        ($func: expr) => {
            BinaryFn::new($func)
        };
    }
}
