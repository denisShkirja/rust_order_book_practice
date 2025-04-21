use std::collections::VecDeque;

pub trait Item {
    fn generation(&self) -> usize;
}

#[derive(Debug)]
pub struct GenerationalDeque<T: Item> {
    buffer: VecDeque<T>,
    start_index: usize,
}

impl<T: Item> GenerationalDeque<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(capacity),
            start_index: 0,
        }
    }

    pub fn push_back(&mut self, item: T) {
        self.buffer.push_back(item);
    }

    pub fn end_index(&self) -> usize {
        self.start_index + self.buffer.len()
    }

    pub fn get(&self, index: usize) -> Option<&T> {
        if index >= self.start_index && index < self.end_index() {
            self.buffer.get(index - self.start_index)
        } else {
            None
        }
    }

    pub fn remove_expired_generations(&mut self, generation: usize) {
        while let Some(item) = self.buffer.front() {
            if item.generation() <= generation {
                self.buffer.pop_front();
                self.start_index += 1;
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct TestItem {
        id: usize,
        generation: usize,
    }

    impl Item for TestItem {
        fn generation(&self) -> usize {
            self.generation
        }
    }

    #[test]
    fn test_new() {
        let deque = GenerationalDeque::<TestItem>::new(5);
        assert_eq!(deque.buffer.capacity(), 5);
        assert_eq!(deque.start_index, 0);
        assert_eq!(deque.buffer.len(), 0);
    }

    #[test]
    fn test_push_back() {
        let mut deque = GenerationalDeque::<TestItem>::new(5);
        let item = TestItem {
            id: 1,
            generation: 1,
        };
        deque.push_back(item.clone());

        assert_eq!(deque.buffer.len(), 1);
        assert_eq!(deque.buffer[0], item);
    }

    #[test]
    fn test_end_index() {
        let mut deque = GenerationalDeque::<TestItem>::new(5);
        assert_eq!(deque.end_index(), 0);

        deque.push_back(TestItem {
            id: 1,
            generation: 1,
        });
        assert_eq!(deque.end_index(), 1);

        deque.push_back(TestItem {
            id: 2,
            generation: 2,
        });
        assert_eq!(deque.end_index(), 2);

        // Simulate removal to increase start_index
        deque.start_index = 1;
        assert_eq!(deque.end_index(), 3);
    }

    #[test]
    fn test_get() {
        let mut deque = GenerationalDeque::<TestItem>::new(5);
        let item1 = TestItem {
            id: 1,
            generation: 1,
        };
        let item2 = TestItem {
            id: 2,
            generation: 2,
        };

        deque.push_back(item1.clone());
        deque.push_back(item2.clone());

        // Test valid indices
        assert_eq!(deque.get(0).unwrap().id, 1);
        assert_eq!(deque.get(1).unwrap().id, 2);

        // Test out of bounds
        assert_eq!(deque.get(2), None);
        assert_eq!(deque.get(usize::MAX), None);

        // Test with modified start_index
        deque.start_index = 10;
        assert_eq!(deque.get(9), None);
        assert_eq!(deque.get(10).unwrap().id, 1);
        assert_eq!(deque.get(11).unwrap().id, 2);
        assert_eq!(deque.get(12), None);
    }

    #[test]
    fn test_remove_expired_generations() {
        let mut deque = GenerationalDeque::<TestItem>::new(5);

        // Add items with different generations
        deque.push_back(TestItem {
            id: 1,
            generation: 1,
        });
        deque.push_back(TestItem {
            id: 2,
            generation: 2,
        });
        deque.push_back(TestItem {
            id: 3,
            generation: 3,
        });
        deque.push_back(TestItem {
            id: 4,
            generation: 4,
        });

        // Initial state
        assert_eq!(deque.start_index, 0);
        assert_eq!(deque.buffer.len(), 4);

        // Remove generations <= 1
        deque.remove_expired_generations(1);
        assert_eq!(deque.start_index, 1);
        assert_eq!(deque.buffer.len(), 3);
        assert_eq!(deque.get(1).unwrap().id, 2);

        // Remove generations <= 2
        deque.remove_expired_generations(2);
        assert_eq!(deque.start_index, 2);
        assert_eq!(deque.buffer.len(), 2);
        assert_eq!(deque.get(2).unwrap().id, 3);

        // Test case where no items should be removed
        deque.remove_expired_generations(2); // Should not change anything
        assert_eq!(deque.start_index, 2);
        assert_eq!(deque.buffer.len(), 2);

        // Remove all remaining items
        deque.remove_expired_generations(5);
        assert_eq!(deque.start_index, 4);
        assert_eq!(deque.buffer.len(), 0);
    }

    #[test]
    fn test_empty_deque() {
        let mut deque = GenerationalDeque::<TestItem>::new(5);

        // Test operations on empty deque
        assert_eq!(deque.get(0), None);
        assert_eq!(deque.end_index(), 0);

        // Should not panic on empty deque
        deque.remove_expired_generations(5);
        assert_eq!(deque.start_index, 0);
    }
}
