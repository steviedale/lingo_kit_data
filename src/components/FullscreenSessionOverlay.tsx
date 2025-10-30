import React, { ReactNode, useEffect, CSSProperties } from "react";

type FullscreenSessionOverlayProps = {
  isOpen: boolean;
  onRequestClose: () => void;
  title?: string;
  children: ReactNode;
  "data-testid"?: string;
};

const overlayStyle: CSSProperties = {
  position: "fixed",
  inset: 0,
  zIndex: 999,
  display: "flex",
  flexDirection: "column",
  backgroundColor: "rgba(15, 23, 42, 0.92)",
  color: "#f8fafc",
};

const headerStyle: CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: "12px",
  padding: "16px 24px",
};

const buttonStyle: CSSProperties = {
  backgroundColor: "rgba(30, 41, 59, 0.9)",
  border: "none",
  borderRadius: "6px",
  padding: "8px 16px",
  fontSize: "0.9rem",
  fontWeight: 600,
  color: "#f8fafc",
  cursor: "pointer",
};

const mainStyle: CSSProperties = {
  flex: 1,
  overflowY: "auto",
  padding: "0 24px 24px",
};

/**
 * A full screen overlay that traps focus inside the session while it is open.
 * This is intended for long running study sessions (flashcards & quizzes).
 */
const FullscreenSessionOverlay: React.FC<FullscreenSessionOverlayProps> = ({
  isOpen,
  onRequestClose,
  title,
  children,
  "data-testid": dataTestId,
}) => {
  useEffect(() => {
    if (!isOpen || typeof document === "undefined") {
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onRequestClose();
      }
    };

    document.addEventListener("keydown", handleKeyDown);

    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";

    return () => {
      document.removeEventListener("keydown", handleKeyDown);
      document.body.style.overflow = previousOverflow;
    };
  }, [isOpen, onRequestClose]);

  if (!isOpen) {
    return null;
  }

  return (
    <div
      style={overlayStyle}
      aria-modal="true"
      role="dialog"
      aria-label={title ?? "Study session"}
      data-testid={dataTestId}
    >
      <header style={headerStyle}>
        <button type="button" onClick={onRequestClose} style={buttonStyle}>
          Exit
        </button>
        {title ? (
          <h2 style={{ margin: 0, fontSize: "1.1rem", fontWeight: 600 }} data-testid="session-overlay-title">
            {title}
          </h2>
        ) : null}
      </header>
      <main style={mainStyle}>{children}</main>
    </div>
  );
};

export default FullscreenSessionOverlay;
