use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use librespot::core::Session;
use librespot::core::SpotifyUri;
use librespot::playback::config::{Bitrate, PlayerConfig};
use librespot::playback::mixer::NoOpVolume;
use librespot::playback::player::{Player, PlayerEvent};
use tokio::sync::mpsc::UnboundedSender;

use crate::stream::channel_sink::{ChannelSink, SinkEvent};
use crate::stream::{StreamError, StreamEvent, StreamEventChannel};
use crate::track::Track;

pub struct Stream {
    player_config: PlayerConfig,
    session: Session,
    retries: u32,
}

impl Stream {
    pub fn new(session: Session, retries: u32) -> Self {
        let config = PlayerConfig {
            bitrate: Bitrate::Bitrate320,
            ..Default::default()
        };
        Stream {
            player_config: config,
            session,
            retries,
        }
    }

    pub async fn stream(&self, track: Track) -> Result<StreamEventChannel> {
        let metadata = track.metadata(&self.session).await?;
        let (sink, mut channel) = ChannelSink::new(metadata);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // Build the list of candidate ids to try, in order of preference: the
        // requested track first, followed by any alternatives (regional
        // re-releases). librespot only falls back to alternatives when the
        // primary track is available but has no files, so we have to resolve
        // region-restricted tracks ourselves.
        let primary_id = track.id.clone();
        let retries = self.retries;
        let mut track_ids = vec![track.id.clone()];
        track_ids.extend(track.alternatives(&self.session).await);

        let player = Player::new(
            self.player_config.clone(),
            self.session.clone(),
            Box::new(NoOpVolume),
            move || Box::new(sink),
        );

        tokio::spawn(async move {
            match tryhard::retry_fn(|| async { Self::load(player.clone(), &track_ids).await })
                .retries(retries)
                .on_retry(|attempt, _, e| {
                    let error = format!("{}", e);
                    let tx = tx.clone();
                    let id = primary_id.clone();
                    async move {
                        tracing::warn!(
                            "Attempt {} to load track {:?} failed: {}",
                            attempt,
                            id,
                            error
                        );
                        Self::send_event(&tx, StreamEvent::Retry {
                            attempt: attempt as usize,
                            max_attempts: retries as usize,
                        }).await;
                    }
                })
                .exponential_backoff(Duration::from_secs(10))
                .max_delay(Duration::from_secs(30))
                .await
            {
                Ok(_) => tracing::info!("Track loaded successfully: {:?}", primary_id),
                Err(e) => {
                    tracing::error!("Failed to load track: {:?}, error: {:?}", primary_id, e);
                    Self::send_event(
                        &tx,
                        StreamEvent::Error(StreamError::LoadError(format!(
                            "Failed to load track: {:?}",
                            primary_id
                        ))),
                    )
                    .await;
                    return;
                }
            }

            tracing::info!("Streaming track: {:?}", primary_id);

            while let Some(event) = channel.recv().await {
                match event {
                    SinkEvent::Write {
                        bytes,
                        total,
                        content,
                    } => {
                        Self::send_event(
                            &tx,
                            StreamEvent::Write {
                                bytes,
                                total,
                                content,
                            },
                        )
                        .await
                    }
                    SinkEvent::Finished => {
                        Self::send_event(&tx, StreamEvent::Finished).await;
                        break;
                    }
                }
            }
        });

        Ok(rx)
    }

    async fn load(player: Arc<Player>, track_ids: &[SpotifyUri]) -> Result<()> {
        let last = track_ids.len().saturating_sub(1);
        for (index, id) in track_ids.iter().enumerate() {
            match Self::load_id(player.clone(), id.clone()).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    if index < last {
                        tracing::warn!(
                            "Track {:?} is unavailable, trying alternative {} of {}",
                            id,
                            index + 1,
                            last
                        );
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        Err(anyhow::anyhow!("No playable track found"))
    }

    async fn load_id(player: Arc<Player>, id: SpotifyUri) -> Result<()> {
        let mut events = player.get_player_event_channel();
        player.load(id.clone(), true, 0);

        tracing::info!("Loading track: {:?}", id);
        loop {
            match events.recv().await {
                Some(PlayerEvent::Playing { .. })
                | Some(PlayerEvent::TrackChanged { .. })
                | Some(PlayerEvent::EndOfTrack { .. }) => {
                    tracing::info!("Player started playing track: {:?}", id);
                    break;
                }
                Some(PlayerEvent::Unavailable { .. }) => {
                    tracing::info!("Track is unavailable: {:?}", id);
                    return Err(anyhow::anyhow!("Could not load track: {:?}", id));
                }
                None => {
                    return Err(anyhow::anyhow!(
                        "Player event channel closed while loading track: {:?}",
                        id
                    ));
                }
                _ => {
                    // Ignore other events
                }
            }
        }

        tokio::spawn(async move {
            player.await_end_of_track().await;
            player.stop();
        });

        Ok(())
    }

    async fn send_event(tx: &UnboundedSender<StreamEvent>, event: StreamEvent) {
        tx.send(event).unwrap_or_else(|e| {
            tracing::error!("Failed to send event: {:?}", e);
        });
    }
}
