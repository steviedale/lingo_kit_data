# Fullscreen session overlay

The flashcard and quiz flows now run inside a fullscreen overlay to prevent users from navigating away mid-session.

## Usage

1. Wrap the part of the application that should support fullscreen sessions with the `FullscreenSessionProvider` (or the `FullscreenSessionBoundary` helper):

```tsx
import { FullscreenSessionBoundary } from "../src/components";

const App = () => (
  <FullscreenSessionBoundary>
    <Routes />
  </FullscreenSessionBoundary>
);
```

2. Use the `FlashcardBatchScreen` and `QuizBatchScreen` helpers to render batch summaries. They accept two render props:

```tsx
<FlashcardBatchScreen
  batchId={batch.id}
  renderBatchOverview={({ startBatch, isLocked }) => (
    <FlashcardBatchOverview
      batch={batch}
      onStart={startBatch}
      disabled={isLocked}
    />
  )}
  renderFlashcardSession={(controls, { batchId }) => (
    <FlashcardSession
      batchId={batchId}
      onComplete={controls.complete}
      onExit={controls.exit}
    />
  )}
/>
```

The overlay presents an Exit button at the top-left corner and closes automatically when `controls.complete()` is called.
