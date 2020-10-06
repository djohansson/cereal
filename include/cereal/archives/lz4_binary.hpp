/*! \file lz4_binary.hpp
    \brief LZ4 compressed Binary input and output archives */
/*
  Copyright (c) 2020, Randolph Voorhies, Shane Grant, Daniel Johansson
  All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are met:
      * Redistributions of source code must retain the above copyright
        notice, this list of conditions and the following disclaimer.
      * Redistributions in binary form must reproduce the above copyright
        notice, this list of conditions and the following disclaimer in the
        documentation and/or other materials provided with the distribution.
      * Neither the name of cereal nor the
        names of its contributors may be used to endorse or promote products
        derived from this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
  DISCLAIMED. IN NO EVENT SHALL RANDOLPH VOORHIES OR SHANE GRANT BE LIABLE FOR ANY
  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#ifndef CEREAL_ARCHIVES_LZ4_BINARY_HPP_
#define CEREAL_ARCHIVES_LZ4_BINARY_HPP_

#include "cereal/cereal.hpp"

#include <lz4frame.h>
#include <lz4hc.h>

#include <cassert>
#include <sstream>

namespace cereal
{
  constexpr size_t getBlockSize(LZ4F_blockSizeID_t blockSizeID)
  {
    switch (blockSizeID)
    {
        case LZ4F_default:
          [[fallthrough]];
        case LZ4F_max64KB:
          return 1 << 16;
        case LZ4F_max256KB:
          return 1 << 18;
        case LZ4F_max1MB:
          return 1 << 20;
        case LZ4F_max4MB:
          return 1 << 22;
        default:
          return 0;
    }
  }

  // ######################################################################
  //! An output archive designed to save data in a compact binary representation
  /*! This archive outputs data to a stream in an extremely compact binary
      representation with as little extra metadata as possible.

      The binary data is compressed using LZ4.

      This archive does nothing to ensure that the endianness of the saved
      and loaded data is the same.  If you need to have portability over
      architectures with different endianness, use PortableBinaryOutputArchive.

      When using a binary archive and a file stream, you must use the
      std::ios::binary format flag to avoid having your data altered
      inadvertently.

      \ingroup Archives */
  class LZ4BinaryOutputArchive : public OutputArchive<LZ4BinaryOutputArchive, AllowEmptyClassElision>
  {
    static constexpr LZ4F_preferences_t sc_lz4Prefs = {
      { LZ4F_default, LZ4F_blockLinked, LZ4F_noContentChecksum, LZ4F_frame, 0, 0, LZ4F_noBlockChecksum },
      0, 0, 0, { 0, 0, 0 } };
    static constexpr size_t sc_lz4InChunkSize = getBlockSize(sc_lz4Prefs.frameInfo.blockSizeID) >> 2;

    public:
      //! Construct, outputting to the provided stream
      /*! @param stream The stream to output to.  Can be a stringstream, a file stream, or
                        even cout! */
      LZ4BinaryOutputArchive(std::ostream & stream) :
        OutputArchive<LZ4BinaryOutputArchive, AllowEmptyClassElision>(this),
        itsStream(stream)
      {
        auto ctxCreateResult = LZ4F_createCompressionContext(&itsLZ4Ctx, LZ4F_VERSION);
        if (LZ4F_isError(ctxCreateResult))
          throw Exception(LZ4F_getErrorName(ctxCreateResult));
        
        itsBuffer.resize(LZ4F_compressBound(getBlockSize(sc_lz4Prefs.frameInfo.blockSizeID), &sc_lz4Prefs));
        itsBufferOffset = LZ4F_compressBegin(itsLZ4Ctx, itsBuffer.data(), itsBuffer.size(), &sc_lz4Prefs);
        if (LZ4F_isError(itsBufferOffset))
          throw Exception(LZ4F_getErrorName(itsBufferOffset));
        
        auto writtenSize = itsStream.rdbuf()->sputn(itsBuffer.data(), itsBufferOffset);
        if (writtenSize != itsBufferOffset)
          throw Exception("Failed to write " + std::to_string(itsBufferOffset) + " bytes to output stream! Wrote " + std::to_string(writtenSize));

        itsBufferOffset = 0;
      }

      ~LZ4BinaryOutputArchive() CEREAL_NOEXCEPT
      {
        auto compressedSize = LZ4F_compressEnd(itsLZ4Ctx, itsBuffer.data() + itsBufferOffset, itsBuffer.size() - itsBufferOffset, nullptr);
        assert(!LZ4F_isError(compressedSize));

        itsBufferOffset += compressedSize;

        assert(itsBufferOffset <= itsBuffer.size());
        
        auto writtenSize = itsStream.rdbuf()->sputn(itsBuffer.data(), itsBufferOffset);
        assert(writtenSize == itsBufferOffset);
        
        auto ctxFreeResult = LZ4F_freeCompressionContext(itsLZ4Ctx);
        assert(!LZ4F_isError(ctxFreeResult));
      }

      //! Writes size bytes of data to the output stream
      void saveBinary(const void * data, std::streamsize size)
      {
        size_t readOffset = 0;
        for (;;)
        {
          auto readSize = std::min(size - readOffset, sc_lz4InChunkSize);
          if (readSize == 0)
            break;

          assert(readSize <= itsBuffer.size());

          if (LZ4F_compressBound(readSize, &sc_lz4Prefs) > (itsBuffer.size() - itsBufferOffset))
            flush();

          auto compressedSize = LZ4F_compressUpdate(
            itsLZ4Ctx,
            itsBuffer.data() + itsBufferOffset,
            itsBuffer.size() - itsBufferOffset,
            reinterpret_cast<const std::byte*>(data) + readOffset,
            readSize,
            nullptr);

          assert(!LZ4F_isError(compressedSize));

          itsBufferOffset += compressedSize;
          readOffset += readSize;
        }
      }

    private:

      void flush()
      {
        auto compressedSize = LZ4F_flush(
          itsLZ4Ctx,
          itsBuffer.data() + itsBufferOffset,
          itsBuffer.size() - itsBufferOffset,
          nullptr);

        assert(!LZ4F_isError(compressedSize));

        itsBufferOffset += compressedSize;

        assert(itsBufferOffset <= itsBuffer.size());
        
        auto const writtenSize = itsStream.rdbuf()->sputn(itsBuffer.data(), itsBufferOffset);
        
        assert(writtenSize == itsBufferOffset);
        
        itsBufferOffset = 0;
      }

      std::ostream & itsStream;
      LZ4F_compressionContext_t itsLZ4Ctx = {};
      std::vector<char> itsBuffer;
      std::vector<char>::size_type itsBufferOffset = 0;
  };

  // ######################################################################
  //! An input archive designed to load data saved using BinaryOutputArchive
  /*  This archive does nothing to ensure that the endianness of the saved
      and loaded data is the same.  If you need to have portability over
      architectures with different endianness, use PortableBinaryOutputArchive.

      The binary data is decompressed using LZ4.

      When using a binary archive and a file stream, you must use the
      std::ios::binary format flag to avoid having your data altered
      inadvertently.

      \ingroup Archives */
  class LZ4BinaryInputArchive : public InputArchive<LZ4BinaryInputArchive, AllowEmptyClassElision>
  {
    public:
      //! Construct, loading from the provided stream
      LZ4BinaryInputArchive(std::istream & stream) :
        InputArchive<LZ4BinaryInputArchive, AllowEmptyClassElision>(this),
        itsStream(stream)
      {
        auto ctxCreateResult = LZ4F_createDecompressionContext(&itsLZ4Ctx, LZ4F_VERSION);
        if (LZ4F_isError(ctxCreateResult))
          throw Exception(LZ4F_getErrorName(ctxCreateResult));

        itsBuffer.resize(LZ4F_HEADER_SIZE_MAX);
        auto readSize = itsStream.rdbuf()->sgetn(itsBuffer.data(), itsBuffer.size());
        if (readSize != itsBuffer.size())
          throw Exception("Failed to read " + std::to_string(itsBuffer.size()) + " bytes from input stream! Read " + std::to_string(readSize));

        auto frameInfo = LZ4F_frameInfo_t{};
        size_t frameSize = itsBuffer.size();
        itsNextSizeHint = LZ4F_getFrameInfo(itsLZ4Ctx, &frameInfo, itsBuffer.data(), &frameSize);
        if (LZ4F_isError(itsNextSizeHint))
          throw Exception(LZ4F_getErrorName(itsNextSizeHint));
        
        itsBuffer.resize(getBlockSize(frameInfo.blockSizeID));
        itsStream.seekg(frameSize);
      }

      ~LZ4BinaryInputArchive() CEREAL_NOEXCEPT
      {
        auto ctxFreeResult = LZ4F_freeDecompressionContext(itsLZ4Ctx);
        assert(!LZ4F_isError(ctxFreeResult));
      }

      //! Reads size bytes of data from the input stream
      void loadBinary( void * const data, std::streamsize size )
      {
        size_t writtenSize = 0;
        while (itsNextSizeHint != 0 && writtenSize < size)
        {
          if (itsBufferOffset == itsLastReadSize)
          {
            itsBufferOffset = 0;
          }
          else if ((itsBufferOffset + itsNextSizeHint) > itsLastReadSize)
          {
            assert(itsLastReadSize < itsBufferOffset);

            itsNextSizeHint = itsLastReadSize - itsBufferOffset;
          }
          
          if (itsBufferOffset == 0)
          {
            itsLastReadSize = itsStream.rdbuf()->sgetn(itsBuffer.data(), itsBuffer.size());
          }

          size_t dstSize = size - writtenSize;
          size_t srcSize = itsNextSizeHint;
          
          assert(srcSize < (itsBuffer.size() - itsBufferOffset));

          itsNextSizeHint = LZ4F_decompress(
            itsLZ4Ctx,
            static_cast<char*>(data) + writtenSize,
            &dstSize,
            itsBuffer.data() + itsBufferOffset,
            &srcSize,
            nullptr);

          assert(!LZ4F_isError(itsNextSizeHint));
          assert(dstSize <= (size - writtenSize));

          writtenSize += dstSize;
          itsBufferOffset += srcSize;

          assert(writtenSize <= size);
          assert(itsBufferOffset <= itsBuffer.size());
        }

        assert(writtenSize == size);
      }

    private:
      std::istream & itsStream;
      LZ4F_decompressionContext_t itsLZ4Ctx = 0;
      std::vector<char> itsBuffer;
      std::vector<char>::size_type itsBufferOffset = 0;
      size_t itsNextSizeHint = 0;
      size_t itsLastReadSize = 0;
  };

  // ######################################################################
  // Common LZ4BinaryArchive serialization functions

  //! Saving for POD types to binary
  template<class T> inline
  typename std::enable_if<std::is_arithmetic<T>::value, void>::type
  CEREAL_SAVE_FUNCTION_NAME(LZ4BinaryOutputArchive & ar, T const & t)
  {
    ar.saveBinary(std::addressof(t), sizeof(t));
  }

  //! Loading for POD types from binary
  template<class T> inline
  typename std::enable_if<std::is_arithmetic<T>::value, void>::type
  CEREAL_LOAD_FUNCTION_NAME(LZ4BinaryInputArchive & ar, T & t)
  {
    ar.loadBinary(std::addressof(t), sizeof(t));
  }

  //! Serializing NVP types to binary
  template <class Archive, class T> inline
  CEREAL_ARCHIVE_RESTRICT(LZ4BinaryInputArchive, LZ4BinaryOutputArchive)
  CEREAL_SERIALIZE_FUNCTION_NAME( Archive & ar, NameValuePair<T> & t )
  {
    ar( t.value );
  }

  //! Serializing SizeTags to binary
  template <class Archive, class T> inline
  CEREAL_ARCHIVE_RESTRICT(LZ4BinaryInputArchive, LZ4BinaryOutputArchive)
  CEREAL_SERIALIZE_FUNCTION_NAME( Archive & ar, SizeTag<T> & t )
  {
    ar( t.size );
  }

  //! Saving binary data
  template <class T> inline
  void CEREAL_SAVE_FUNCTION_NAME(LZ4BinaryOutputArchive & ar, BinaryData<T> const & bd)
  {
    ar.saveBinary( bd.data, static_cast<std::streamsize>( bd.size ) );
  }

  //! Loading binary data
  template <class T> inline
  void CEREAL_LOAD_FUNCTION_NAME(LZ4BinaryInputArchive & ar, BinaryData<T> & bd)
  {
    ar.loadBinary(bd.data, static_cast<std::streamsize>( bd.size ) );
  }
} // namespace cereal

// register archives for polymorphic support
CEREAL_REGISTER_ARCHIVE(cereal::LZ4BinaryOutputArchive)
CEREAL_REGISTER_ARCHIVE(cereal::LZ4BinaryInputArchive)

// tie input and output archives together
CEREAL_SETUP_ARCHIVE_TRAITS(cereal::LZ4BinaryInputArchive, cereal::LZ4BinaryOutputArchive)

#endif // CEREAL_ARCHIVES_LZ4_BINARY_HPP_
