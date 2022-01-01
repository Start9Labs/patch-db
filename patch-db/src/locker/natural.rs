#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Natural(usize);
impl Natural {
    pub fn one() -> Self {
        Natural(1)
    }
    pub fn of(n: usize) -> Option<Self> {
        if n == 0 {
            None
        } else {
            Some(Natural(n))
        }
    }
    pub fn inc(&mut self) {
        self.0 += 1;
    }
    pub fn dec(mut self) -> Option<Natural> {
        self.0 -= 1;
        if self.0 == 0 {
            None
        } else {
            Some(self)
        }
    }
    pub fn into_usize(self) -> usize {
        self.0
    }
}
