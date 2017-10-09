{-| This module provides a way to use concurrent haskell
    inside of 'ST'. Using these function subverts the
    usual guarentee we have that 'ST' is deterministic.
-}

{-# LANGUAGE UnboxedTuples #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE BangPatterns #-}

{-# OPTIONS_GHC -Wall #-}

module Control.Concurrent.ST
  ( -- * Threads
    ThreadId(..)
  , forkST
  , forkST_
    -- * MVar
  , MVar
  , newEmptyMVar
  , newMVar
  , takeMVar
  , putMVar
  , readMVar
  , tryTakeMVar
  , tryPutMVar
  , isEmptyMVar
  , tryReadMVar
    -- * Parallelism
  , tandem
  , traverse_
  , foldCommuteM
  ) where

import GHC.Prim
import GHC.Exts (isTrue#)
import GHC.ST (ST(..))
import Data.Foldable (foldlM,Foldable)
import Control.Monad (replicateM_)

data ThreadId s = ThreadId ThreadId#

-- | Creates a new thread to run the 'ST' computation passed
--   as the argument. Since using the 'ThreadId' often
--   leads to non-determinism, the function 'forkST_'
--   is typically to be preferred.
forkST :: ST s a -> ST s (ThreadId s)
{-# INLINE forkST #-}
forkST action = ST $ \s1 -> case forkST# action s1 of
  (# s2, tid #) -> (# s2, ThreadId tid #)

-- | Creates a new thread to run the 'ST' computation and
--   discard the 'ThreadId'.
forkST_ :: ST s a -> ST s ()
{-# INLINE forkST_ #-}
forkST_ action = ST $ \s1 -> case forkST# action s1 of
  (# s2, _ #) -> (# s2, () #)

forkST# :: a -> State# s -> (# State# s, ThreadId# #)
forkST# = unsafeCoerce# fork#

data MVar s a = MVar (MVar# s a)

instance Eq (MVar s a) where
  (MVar mvar1#) == (MVar mvar2#) = isTrue# (sameMVar# mvar1# mvar2#)

newEmptyMVar :: ST s (MVar s a)
newEmptyMVar = ST $ \s1 -> case newMVar# s1 of
  (# s2, v #) -> (# s2, MVar v #)

takeMVar :: MVar s a -> ST s a
takeMVar (MVar mvar#) = ST $ \ s# -> takeMVar# mvar# s#

putMVar  :: MVar s a -> a -> ST s ()
putMVar (MVar mvar#) x = ST $ \ s# ->
  case putMVar# mvar# x s# of
    s2# -> (# s2#, () #)

tryTakeMVar :: MVar s a -> ST s (Maybe a)
tryTakeMVar (MVar m) = ST $ \ s ->
    case tryTakeMVar# m s of
        (# s', 0#, _ #) -> (# s', Nothing #) -- MVar is empty
        (# s', _,  a #) -> (# s', Just a  #) -- MVar is full

tryPutMVar :: MVar s a -> a -> ST s Bool
tryPutMVar (MVar mvar#) x = ST $ \ s# ->
    case tryPutMVar# mvar# x s# of
        (# s, 0# #) -> (# s, False #)
        (# s, _  #) -> (# s, True #)

tryReadMVar :: MVar s a -> ST s (Maybe a)
tryReadMVar (MVar m) = ST $ \ s ->
  case tryReadMVar# m s of
    (# s', 0#, _ #) -> (# s', Nothing #) -- MVar is empty
    (# s', _,  a #) -> (# s', Just a  #) -- MVar is full

isEmptyMVar :: MVar s a -> ST s Bool
isEmptyMVar (MVar mv#) = ST $ \ s# ->
  case isEmptyMVar# mv# s# of
    (# s2#, flg #) -> (# s2#, isTrue# (flg /=# 0#) #)

newMVar :: a -> ST s (MVar s a)
newMVar value = do
  mvar <- newEmptyMVar
  putMVar mvar value
  return mvar

readMVar :: MVar s a -> ST s a
readMVar (MVar mvar#) = ST $ \ s# -> readMVar# mvar# s#

-- | Execute the first computation on the main thread and
--   the second one on another thread in parallel. Blocks
--   until both are finished.
tandem :: ST s a -> ST s b -> ST s a
tandem a b = do
  lock <- newEmptyMVar
  forkST_ (b >> putMVar lock ())
  x <- a
  takeMVar lock
  return x

-- | Fold over a collection in parallel, discarding the results.
traverse_ :: Foldable t => (a -> ST s b) -> t a -> ST s ()
traverse_ f xs = do
  var <- newEmptyMVar
  total <- foldlM (\ !n a -> forkST_ (f a >> putMVar var ()) >> return (n + 1)) 0 xs
  replicateM_ total (takeMVar var)

-- | A more performant variant of 'foldMapM' that is only valid
--   for commutative monoids.
foldCommuteM :: (Foldable t, Monoid m) => (a -> ST s m) -> t a -> ST s m
foldCommuteM f xs = do
  var <- newEmptyMVar
  total <- foldlM (\ !n a -> forkST_ (f a >>= putMVar var) >> return (n + 1)) 0 xs
  let go !n !m = if (n :: Int) < total
        then takeMVar var >>= go (n + 1)
        else return m
  go 0 mempty

