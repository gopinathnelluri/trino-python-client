
import unittest
import queue
import time
from unittest.mock import MagicMock, patch
from trino.client import ThreadedSegmentIterator, SpooledSegment, DecodableSegment

class TestThreadedSegmentIterator(unittest.TestCase):
    def test_prefetch_order_and_threading(self):
        # Create mock segments
        mock_segments = []
        for i in range(10):
            seg = MagicMock(spec=DecodableSegment)
            seg.encoding = "json"
            # Mock the segment object inside DecodableSegment
            mock_spooled_seg = MagicMock(spec=SpooledSegment)
            seg.segment = mock_spooled_seg 
            mock_segments.append(seg)

        # Mock mapper
        mapper = MagicMock()
        
        # We need to mock SegmentDecoder because ThreadedSegmentIterator instantiates it locally
        with patch("trino.client.SegmentDecoder") as MockDecoder:
            # Setup decoder to return "decoded" data based on segment
            # We can use side_effect to return different data for each call
            # But the decoder is instantiated PER segment in the loop.
            
            # The code does:
            # decoder = SegmentDecoder(Factory.create(segment.encoding))
            # rows = decoder.decode(segment.segment)
            
            # So we need MockDecoder.return_value.decode.return_value to be the rows.
            # But we want rows to match the segment order.
            
            # Let's make decode return a value based on the input segment
            def decode_side_effect(seg_data):
                # seg_data is the spooled segment mock
                # Let's verify we process them in order
                index = -1
                for idx, m in enumerate(mock_segments):
                    if m.segment == seg_data:
                        index = idx
                        break
                return [[f"row_{index}"]]

            MockDecoder.return_value.decode.side_effect = decode_side_effect
            
            # Initialize iterator with 2 threads and buffer size 3
            iterator = ThreadedSegmentIterator(mock_segments, mapper, max_workers=2, buffer_size=3)
            
            # Consume rows and verify order
            results = []
            for row in iterator:
                results.append(row)
                
            # Verify we got all results in order
            expected = [[f"row_{i}"] for i in range(10)]
            self.assertEqual(results, expected)
            
            # Verify stop
            self.assertTrue(iterator._finished)
            
            # Verify clean shutdown
            iterator.close()
            
    def test_exception_propagation(self):
        # Create mock segments
        mock_segments = [MagicMock(spec=DecodableSegment)]
        mock_segments[0].encoding = "json"
        mock_segments[0].segment = MagicMock(spec=SpooledSegment)
        
        mapper = MagicMock()
        
        with patch("trino.client.SegmentDecoder") as MockDecoder:
            # Make decode raise exception
            MockDecoder.return_value.decode.side_effect = Exception("Download failed")
            
            iterator = ThreadedSegmentIterator(mock_segments, mapper, max_workers=1)
            
            with self.assertRaisesRegex(Exception, "Download failed"):
                next(iterator)
                
            iterator.close()

if __name__ == '__main__':
    unittest.main()
