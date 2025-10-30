import { ReactNode, useCallback, useMemo, useState } from "react";

export type SessionDescriptor = {
  title?: string;
  render: (controls: { complete: () => void; exit: () => void }) => ReactNode;
};

export type FullscreenSessionController = {
  session: SessionDescriptor | null;
  isOpen: boolean;
  open: (descriptor: SessionDescriptor) => void;
  exit: () => void;
  complete: () => void;
};

export const useFullscreenSession = (): FullscreenSessionController => {
  const [session, setSession] = useState<SessionDescriptor | null>(null);

  const open = useCallback((descriptor: SessionDescriptor) => {
    setSession(descriptor);
  }, []);

  const exit = useCallback(() => {
    setSession(null);
  }, []);

  const complete = useCallback(() => {
    setSession(null);
  }, []);

  return useMemo(
    () => ({
      session,
      isOpen: Boolean(session),
      open,
      exit,
      complete,
    }),
    [complete, exit, open, session]
  );
};
