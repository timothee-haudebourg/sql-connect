use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use futures_timer::Delay;
use ::backoff::backoff::{Backoff};
use crate::{
	ErrorKind,
	Result
};

pub struct BackoffState<B> {
	delay: Option<Delay>,
	backoff: B,
}

impl<B: Backoff + Unpin> BackoffState<B> {
	pub fn new(backoff: B) -> BackoffState<B> {
		BackoffState {
			delay: None,
			backoff
		}
	}

	unsafe_pinned!(delay: Option<Delay>);

	pub fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Result<()> {
		loop {
			match self.backoff.next_backoff() {
				Some(duration) => {
					let mut delay = self.as_mut().delay();
					if delay.is_none() {
						delay.replace(Delay::new(duration));
						let mut delay = delay.as_pin_mut().unwrap();
						delay.reset(duration);
						match delay.poll(cx) {
							Poll::Ready(()) => (),
							Poll::Pending => break
						}
					} else {
						let mut delay = delay.as_pin_mut().unwrap();
						delay.reset(duration);
						match delay.poll(cx) {
							Poll::Ready(()) => (),
							Poll::Pending => break
						}
					}
				},
				None => return Err(ErrorKind::Busy.err())
			}
		}

		Ok(())
	}
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct BackoffFuture<'b, F, B> {
	delay: Option<Delay>,
	backoff: &'b mut B,
	future: F,
}

impl<'b, F, B> BackoffFuture<'b, F, B> {
	unsafe_pinned!(future: F);
	unsafe_pinned!(delay: Option<Delay>);
	unsafe_pinned!(backoff: &'b mut B);
}

pub trait BackoffExt<F> {
	fn with_backoff<B: Backoff>(self, backoff: &mut B) -> BackoffFuture<'_, F, B>;
}

impl<F> BackoffExt<F> for F {
	fn with_backoff<B: Backoff>(self, backoff: &mut B) -> BackoffFuture<F, B> {
		BackoffFuture {
			delay: None,
			backoff,
			future: self
		}
	}
}

impl<F, B, T> Future for BackoffFuture<'_, F, B>
	where F: Future<Output = Result<T>>,
		  B: Backoff
{
	type Output = F::Output;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		loop {
			match self.as_mut().future().poll(cx) {
				Poll::Ready(Err(e)) if e.kind().is_busy() => {
					match self.as_mut().backoff().next_backoff() {
						Some(duration) => {
							let mut delay = self.as_mut().delay();
							if delay.is_none() {
								delay.replace(Delay::new(duration));
							} else {
								let mut delay = delay.as_pin_mut().unwrap();
								delay.reset(duration);
								match delay.poll(cx) {
									Poll::Ready(()) => (),
									Poll::Pending => return Poll::Pending
								}
							}
						},
						None => return Poll::Ready(Err(e))
					}
				},
				Poll::Ready(result) => return Poll::Ready(result),
				Poll::Pending => return Poll::Pending
			}
		}
	}
}
