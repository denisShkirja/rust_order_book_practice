use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

#[derive(Debug)]
struct BatchHeader {
    len: usize,
    is_removed: bool,
}

#[derive(Debug)]
struct Item<T> {
    data: T,
    batch_header: Option<BatchHeader>,
}

#[derive(Debug)]
pub struct BatchedDeque<T> {
    state: Rc<RefCell<BatchedDequeState<T>>>,
}

impl<T> BatchedDeque<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Rc::new(RefCell::new(BatchedDequeState::new(capacity))),
        }
    }

    pub fn push_back_batch<E, I: Iterator<Item = Result<T, E>>>(
        &self,
        iter: I,
    ) -> Result<BatchGuard<T>, E> {
        let batch = self.state.borrow_mut().push_back_batch(iter)?;
        Ok(BatchGuard {
            deque: self.state.clone(),
            batch,
        })
    }
}

#[derive(Debug)]
struct BatchedDequeState<T> {
    buffer: VecDeque<Item<T>>,
    start_index: usize,
}

#[derive(Debug)]
pub struct BatchGuard<T> {
    deque: Rc<RefCell<BatchedDequeState<T>>>,
    batch: Batch,
}

impl<T> BatchGuard<T> {
    pub fn for_each<E>(&self, mut f: impl FnMut(&T) -> Result<(), E>) -> Result<(), E> {
        let deque = self.deque.borrow();
        for i in 0..self.batch.len {
            let index = self.batch.start_index + i;
            let item = deque.get(index);
            assert!(item.is_some(), "Expected item to exist at index {}", index);
            f(item.unwrap())?;
        }
        Ok(())
    }
}

impl<T> Drop for BatchGuard<T> {
    fn drop(&mut self) {
        let mut deque = self.deque.borrow_mut();
        deque.remove_batch(&self.batch);
    }
}

#[derive(Debug, Clone, Copy)]
struct Batch {
    start_index: usize,
    len: usize,
}

impl<T> BatchedDequeState<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(capacity),
            start_index: 0,
        }
    }

    pub fn push_back_batch<E, I: Iterator<Item = Result<T, E>>>(
        &mut self,
        iter: I,
    ) -> Result<Batch, E> {
        let batch_start = self.buffer.len();
        let mut batch_len = 0;
        for item in iter {
            match item {
                Ok(item) => {
                    self.buffer.push_back(Item {
                        data: item,
                        batch_header: None,
                    });
                    batch_len += 1;
                }
                Err(e) => {
                    self.buffer.truncate(batch_start);
                    return Err(e);
                }
            }
        }
        if let Some(header) = self.buffer.get_mut(batch_start) {
            header.batch_header = Some(BatchHeader {
                len: batch_len,
                is_removed: false,
            });
        }
        Ok(Batch {
            start_index: batch_start + self.start_index,
            len: batch_len,
        })
    }

    fn convert_to_deque_index(&self, index: usize) -> Option<usize> {
        if index >= self.start_index && index < self.start_index + self.buffer.len() {
            Some(index - self.start_index)
        } else {
            None
        }
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        self.convert_to_deque_index(index)
            .and_then(|deque_index| self.buffer.get(deque_index))
            .map(|item| &item.data)
    }

    pub fn remove_batch(&mut self, batch: &Batch) {
        let deque_index = match self.convert_to_deque_index(batch.start_index) {
            Some(idx) => idx,
            None => return,
        };

        let should_perform_cleanup = {
            let item = match self.buffer.get_mut(deque_index) {
                Some(item) => item,
                None => return,
            };

            let batch_header = match &mut item.batch_header {
                Some(header) if header.len == batch.len => header,
                _ => return,
            };

            batch_header.is_removed = true;
            deque_index == 0
        };

        if should_perform_cleanup {
            self.cleanup_removed_batchs();
        }
    }

    fn cleanup_removed_batchs(&mut self) {
        while let Some(front) = self.buffer.front() {
            if let Some(header) = &front.batch_header {
                if header.is_removed {
                    let batch_len = header.len;
                    assert!(
                        batch_len <= self.buffer.len(),
                        "Batch length is greater than the buffer length"
                    );
                    self.buffer.drain(0..batch_len);
                    self.start_index += batch_len;
                    continue;
                }
            }
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_batched_deque() {
        let deque = BatchedDeque::<i32>::new(10);
        assert_eq!(deque.state.borrow().buffer.capacity(), 10);
        assert_eq!(deque.state.borrow().start_index, 0);
    }

    #[test]
    fn test_push_back_batch() {
        let deque = BatchedDeque::<i32>::new(10);
        let data = [1, 2, 3, 4, 5];
        let batch_guard = deque
            .push_back_batch(data.iter().map(|&x| Ok::<i32, ()>(x)))
            .unwrap();

        // Check batch properties
        assert_eq!(batch_guard.batch.start_index, 0);
        assert_eq!(batch_guard.batch.len, 5);

        // Check deque state
        let state = deque.state.borrow();
        assert_eq!(state.buffer.len(), 5);
        assert_eq!(state.start_index, 0);

        // Check first item has batch header
        if let Some(header) = &state.buffer[0].batch_header {
            assert_eq!(header.len, 5);
            assert!(!header.is_removed);
        } else {
            panic!("Expected batch header on first item");
        }

        // Check all other items have no batch header
        for i in 1..5 {
            assert!(state.buffer[i].batch_header.is_none());
        }
    }

    #[test]
    fn test_batch_guard_for_each() {
        let deque = BatchedDeque::<i32>::new(10);
        let data = [1, 2, 3, 4, 5];
        let batch_guard = deque
            .push_back_batch(data.iter().map(|&x| Ok::<i32, ()>(x)))
            .unwrap();

        let mut vec = Vec::new();
        batch_guard
            .for_each(|&item| {
                vec.push(item);
                Ok::<(), ()>(())
            })
            .unwrap();

        assert_eq!(vec, data);
    }

    #[test]
    fn test_batch_guard_drop() {
        let deque = BatchedDeque::<i32>::new(10);

        {
            let data = [1, 2, 3, 4, 5];
            let _batch_guard = deque
                .push_back_batch(data.iter().map(|&x| Ok::<i32, ()>(x)))
                .unwrap();

            assert_eq!(deque.state.borrow().buffer.len(), 5);
        }

        assert_eq!(deque.state.borrow().buffer.len(), 0);
        assert_eq!(deque.state.borrow().start_index, 5);
    }

    #[test]
    fn test_multiple_batches_one_after_another() {
        let deque = BatchedDeque::<i32>::new(20);

        // Add first batch
        let data1 = [1, 2, 3];
        let batch_guard1 = deque
            .push_back_batch(data1.iter().map(|&x| Ok::<i32, ()>(x)))
            .unwrap();

        assert_eq!(deque.state.borrow().buffer.len(), 3);
        assert_eq!(deque.state.borrow().start_index, 0);

        // Verify first batch
        let mut vec1 = Vec::new();
        batch_guard1
            .for_each(|&item| {
                vec1.push(item);
                Ok::<(), ()>(())
            })
            .unwrap();
        assert_eq!(vec1, data1);

        // Drop first batch
        drop(batch_guard1);

        assert_eq!(deque.state.borrow().buffer.len(), 0);
        assert_eq!(deque.state.borrow().start_index, 3);

        // Add second batch
        let data2 = [4, 5, 6, 7];
        let batch_guard2 = deque
            .push_back_batch(data2.iter().map(|&x| Ok::<i32, ()>(x)))
            .unwrap();

        // Check both batches are in deque
        assert_eq!(deque.state.borrow().buffer.len(), 4);
        assert_eq!(deque.state.borrow().start_index, 3);

        // Verify second batch
        let mut vec2 = Vec::new();
        batch_guard2
            .for_each(|&item| {
                vec2.push(item);
                Ok::<(), ()>(())
            })
            .unwrap();
        assert_eq!(vec2, data2);

        // Drop second batch
        drop(batch_guard2);

        assert_eq!(deque.state.borrow().buffer.len(), 0);
        assert_eq!(deque.state.borrow().start_index, 7);
    }

    #[test]
    fn test_multiple_batches_forward_cleanup() {
        let deque = BatchedDeque::<i32>::new(20);

        // Add first batch
        let data1 = [1, 2, 3];
        let batch_guard1 = deque
            .push_back_batch(data1.iter().map(|&x| Ok::<i32, ()>(x)))
            .unwrap();

        // Add second batch
        let data2 = [4, 5, 6, 7];
        let batch_guard2 = deque
            .push_back_batch(data2.iter().map(|&x| Ok::<i32, ()>(x)))
            .unwrap();

        // Check both batches are in deque
        assert_eq!(deque.state.borrow().buffer.len(), 7);
        assert_eq!(deque.state.borrow().start_index, 0);

        // Verify first batch
        let mut vec1 = Vec::new();
        batch_guard1
            .for_each(|&item| {
                vec1.push(item);
                Ok::<(), ()>(())
            })
            .unwrap();
        assert_eq!(vec1, data1);

        // Verify second batch
        let mut vec2 = Vec::new();
        batch_guard2
            .for_each(|&item| {
                vec2.push(item);
                Ok::<(), ()>(())
            })
            .unwrap();
        assert_eq!(vec2, data2);

        // Drop first batch
        drop(batch_guard1);

        assert_eq!(deque.state.borrow().buffer.len(), 4);
        assert_eq!(deque.state.borrow().start_index, 3);

        // Drop second batch
        drop(batch_guard2);

        assert_eq!(deque.state.borrow().buffer.len(), 0);
        assert_eq!(deque.state.borrow().start_index, 7);
    }

    #[test]
    fn test_multiple_batches_reverse_cleanup() {
        let deque = BatchedDeque::<i32>::new(20);

        // Add first batch
        let data1 = [1, 2, 3];
        let batch_guard1 = deque
            .push_back_batch(data1.iter().map(|&x| Ok::<i32, ()>(x)))
            .unwrap();

        // Add second batch
        let data2 = [4, 5, 6, 7];
        let batch_guard2 = deque
            .push_back_batch(data2.iter().map(|&x| Ok::<i32, ()>(x)))
            .unwrap();

        // Check both batches are in deque
        assert_eq!(deque.state.borrow().buffer.len(), 7);
        assert_eq!(deque.state.borrow().start_index, 0);

        // Verify first batch
        let mut vec1 = Vec::new();
        batch_guard1
            .for_each(|&item| {
                vec1.push(item);
                Ok::<(), ()>(())
            })
            .unwrap();
        assert_eq!(vec1, data1);

        // Verify second batch
        let mut vec2 = Vec::new();
        batch_guard2
            .for_each(|&item| {
                vec2.push(item);
                Ok::<(), ()>(())
            })
            .unwrap();
        assert_eq!(vec2, data2);

        // Drop second batch
        drop(batch_guard2);

        assert_eq!(deque.state.borrow().buffer.len(), 7);
        assert_eq!(deque.state.borrow().start_index, 0);

        // Drop first batch
        drop(batch_guard1);

        assert_eq!(deque.state.borrow().buffer.len(), 0);
        assert_eq!(deque.state.borrow().start_index, 7);
    }

    #[test]
    fn test_error_during_batch_creation() {
        let deque = BatchedDeque::<i32>::new(10);

        // Create a vector of Results
        let data: Vec<Result<i32, &'static str>> =
            vec![Ok(0), Ok(1), Ok(2), Err("Error occurred"), Ok(4)];

        // Batch creation should fail
        let result = deque.push_back_batch(data.into_iter());
        assert!(result.is_err());

        // Deque should be empty due to rollback
        assert_eq!(deque.state.borrow().buffer.len(), 0);
    }

    #[test]
    fn test_empty_batch() {
        let deque = BatchedDeque::<i32>::new(10);
        let empty_vec: Vec<Result<i32, ()>> = vec![];

        // Empty batch should be created successfully
        let batch_guard = deque.push_back_batch(empty_vec.into_iter()).unwrap();

        // Check batch properties
        assert_eq!(batch_guard.batch.start_index, 0);
        assert_eq!(batch_guard.batch.len, 0);

        // Check deque state
        assert_eq!(deque.state.borrow().buffer.len(), 0);
        assert_eq!(deque.state.borrow().start_index, 0);

        // for_each should not iterate over any items
        let mut vec = Vec::new();
        batch_guard
            .for_each(|&item| {
                vec.push(item);
                Ok::<(), ()>(())
            })
            .unwrap();
        assert!(vec.is_empty());

        // Dropping the batch should not change the state
        drop(batch_guard);
        assert_eq!(deque.state.borrow().buffer.len(), 0);
        assert_eq!(deque.state.borrow().start_index, 0);
    }
}
