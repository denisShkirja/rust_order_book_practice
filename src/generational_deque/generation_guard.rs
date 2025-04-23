use crate::generational_deque::generational_deque::GenerationalDeque;
use crate::generational_deque::generational_deque::Item;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Debug)]
pub struct GenerationGuard<T: Item> {
    deque: Option<Rc<RefCell<GenerationalDeque<T>>>>,
    start_index: usize,
    count: usize,
    generation: usize,
}

impl<T: Item> GenerationGuard<T> {
    pub fn new(
        deque: Rc<RefCell<GenerationalDeque<T>>>,
        start_index: usize,
        count: usize,
        generation: usize,
    ) -> Self {
        Self {
            deque: Some(deque),
            start_index,
            count,
            generation,
        }
    }

    pub fn drop_ownership(&mut self) {
        self.deque = None;
    }

    pub fn for_each<E>(&self, mut f: impl FnMut(&T) -> Result<(), E>) -> Result<(), E> {
        if let Some(deque) = &self.deque {
            let deque = deque.borrow();
            for i in 0..self.count {
                let index = self.start_index + i;
                if let Some(item) = deque.get(index) {
                    f(item)?;
                }
            }
        }
        Ok(())
    }
}

impl<T: Item> Drop for GenerationGuard<T> {
    fn drop(&mut self) {
        if let Some(deque) = &self.deque {
            deque
                .borrow_mut()
                .remove_expired_generations(self.generation);
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

    fn setup_test_deque() -> Rc<RefCell<GenerationalDeque<TestItem>>> {
        let mut deque = GenerationalDeque::new(10);

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

        Rc::new(RefCell::new(deque))
    }

    #[test]
    fn test_generation_guard_creation() {
        let deque = setup_test_deque();
        let guard = GenerationGuard::new(deque.clone(), 0, 4, 2);

        assert_eq!(guard.start_index, 0);
        assert_eq!(guard.count, 4);
        assert_eq!(guard.generation, 2);
    }

    #[test]
    fn test_for_each() {
        let deque = setup_test_deque();
        let guard = GenerationGuard::new(deque.clone(), 0, 4, 2);

        // Collect IDs to verify for_each visits all items
        let mut collected_ids = Vec::new();
        guard
            .for_each(|item| {
                collected_ids.push(item.id);
                Ok::<(), ()>(())
            })
            .unwrap();

        assert_eq!(collected_ids, vec![1, 2, 3, 4]);

        // Test with offset start_index
        let guard2 = GenerationGuard::new(deque.clone(), 1, 1, 2);
        let mut collected_ids2 = Vec::new();
        guard2
            .for_each(|item| {
                collected_ids2.push(item.id);
                Ok::<(), ()>(())
            })
            .unwrap();

        assert_eq!(collected_ids2, vec![2]);
    }

    #[test]
    fn test_for_each_early_return() {
        let deque = setup_test_deque();
        let guard = GenerationGuard::new(deque.clone(), 0, 4, 2);

        let mut collected_ids = Vec::new();
        let result = guard.for_each(|item| {
            collected_ids.push(item.id);
            if item.id == 2 {
                return Err("stopped at 2");
            }
            Ok::<(), &str>(())
        });

        assert!(result.is_err());
        assert_eq!(collected_ids, vec![1, 2]);
    }

    #[test]
    fn test_drop_removes_expired_generations() {
        let deque = setup_test_deque();

        // Before drop
        assert_eq!(deque.borrow().get(0).unwrap().id, 1);
        assert_eq!(deque.borrow().get(1).unwrap().id, 2);
        assert_eq!(deque.borrow().get(2).unwrap().id, 3);
        assert_eq!(deque.borrow().get(3).unwrap().id, 4);

        {
            // Create guard with generation 2 (should remove generations <= 2 when dropped)
            let _guard = GenerationGuard::new(deque.clone(), 0, 4, 2);
            // Guard is dropped at the end of this scope
        }

        // After drop, first two items should be removed
        assert!(deque.borrow().get(0).is_none());
        assert!(deque.borrow().get(1).is_none());
        assert_eq!(deque.borrow().get(2).unwrap().id, 3);
        assert_eq!(deque.borrow().get(3).unwrap().id, 4);
    }

    #[test]
    fn test_multiple_guards() {
        let deque = setup_test_deque();

        {
            // Create first guard with generation 1
            let _guard1 = GenerationGuard::new(deque.clone(), 0, 1, 1);

            {
                // Create second guard with generation 3
                let _guard2 = GenerationGuard::new(deque.clone(), 2, 1, 3);
                // guard2 is dropped here - should remove generations <= 3
            }

            // After guard2 drop, first three items should be removed
            assert!(deque.borrow().get(0).is_none());
            assert!(deque.borrow().get(1).is_none());
            assert!(deque.borrow().get(2).is_none());
            assert_eq!(deque.borrow().get(3).unwrap().id, 4);

            // guard1 is dropped here, but all items <= generation 1 are already removed
        }
    }

    #[test]
    fn test_unordered_generations() {
        let deque = Rc::new(RefCell::new(GenerationalDeque::new(10)));
        deque.borrow_mut().push_back(TestItem {
            id: 3,
            generation: 3,
        });
        deque.borrow_mut().push_back(TestItem {
            id: 2,
            generation: 2,
        });
        deque.borrow_mut().push_back(TestItem {
            id: 1,
            generation: 1,
        });

        // Drop generation 1, nothing should be removed
        {
            let _guard1 = GenerationGuard::new(deque.clone(), 2, 1, 1);
        }
        assert_eq!(deque.borrow().get(0).unwrap().id, 3);
        assert_eq!(deque.borrow().get(1).unwrap().id, 2);
        assert_eq!(deque.borrow().get(2).unwrap().id, 1);

        // Drop generation 2, nothing should be removed
        {
            let _guard1 = GenerationGuard::new(deque.clone(), 1, 1, 2);
        }
        assert_eq!(deque.borrow().get(0).unwrap().id, 3);
        assert_eq!(deque.borrow().get(1).unwrap().id, 2);
        assert_eq!(deque.borrow().get(2).unwrap().id, 1);

        // Drop generation 3, all items should be removed
        {
            let _guard1 = GenerationGuard::new(deque.clone(), 0, 1, 3);
        }
        assert!(deque.borrow().get(0).is_none());
        assert!(deque.borrow().get(1).is_none());
        assert!(deque.borrow().get(2).is_none());
    }

    #[test]
    fn test_drop_ownership() {
        let deque = setup_test_deque();

        // Confirm initial state
        assert_eq!(deque.borrow().get(0).unwrap().id, 1);
        assert_eq!(deque.borrow().get(1).unwrap().id, 2);
        assert_eq!(deque.borrow().get(2).unwrap().id, 3);
        assert_eq!(deque.borrow().get(3).unwrap().id, 4);

        {
            let mut guard = GenerationGuard::new(deque.clone(), 3, 1, 3);
            guard.drop_ownership();
        }

        // Verify items are still in the deque (nothing was removed)
        assert_eq!(deque.borrow().get(0).unwrap().id, 1);
        assert_eq!(deque.borrow().get(1).unwrap().id, 2);
        assert_eq!(deque.borrow().get(2).unwrap().id, 3);
        assert_eq!(deque.borrow().get(3).unwrap().id, 4);
    }
}
