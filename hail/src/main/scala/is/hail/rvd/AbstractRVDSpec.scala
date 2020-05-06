package is.hail.rvd

import is.hail.annotations._
import is.hail.expr.JSONAnnotationImpex
import is.hail.expr.ir.ExecuteContext
import is.hail.expr.types.encoded.{EType, ETypeSerializer}
import is.hail.expr.types.physical.{PCanonicalStruct, PInt64Optional, PInt64Required, PStruct, PType, PTypeSerializer}
import is.hail.expr.types.virtual.{TStructSerializer, _}
import is.hail.io._
import is.hail.io.fs.FS
import is.hail.io.index.{InternalNodeBuilder, LeafNodeBuilder}
import is.hail.utils._
import is.hail.{HailContext, compatibility}
import org.apache.spark.TaskContext
import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{DefaultFormats, Formats, JValue, ShortTypeHints}

object AbstractRVDSpec {
  implicit val formats: Formats = new DefaultFormats() {
    override val typeHints = ShortTypeHints(List(
      classOf[AbstractRVDSpec],
      classOf[OrderedRVDSpec2],
      classOf[IndexedRVDSpec2],
      classOf[IndexSpec2],
      classOf[compatibility.OrderedRVDSpec],
      classOf[compatibility.IndexedRVDSpec],
      classOf[compatibility.IndexSpec],
      classOf[compatibility.UnpartitionedRVDSpec],
      classOf[AbstractTypedCodecSpec],
      classOf[TypedCodecSpec])
    ) + BufferSpec.shortTypeHints
    override val typeHintFieldName = "name"
  }  +
    new TStructSerializer +
    new TypeSerializer +
    new PTypeSerializer +
    new RVDTypeSerializer +
    new ETypeSerializer

  def read(fs: FS, path: String): AbstractRVDSpec = {
    val metadataFile = path + "/metadata.json.gz"
    using(fs.open(metadataFile)) { in => JsonMethods.parse(in) }
      .transformField { case ("orvdType", value) => ("rvdType", value) } // ugh
      .extract[AbstractRVDSpec]
  }

  def readLocal(
    ctx: ExecuteContext,
    path: String,
    enc: AbstractTypedCodecSpec,
    partFiles: Array[String],
    requestedType: TStruct): (PStruct, Long) = {
    assert(partFiles.length == 1)
    val fs = ctx.fs
    val r = ctx.r

    val (rType: PStruct, dec) = enc.buildDecoder(ctx, requestedType)

    val f = partPath(path, partFiles(0))
    using(fs.open(f)) { in =>
      val Array(rv) = HailContext.readRowsPartition(dec)(r, in).toArray
      (rType, rv)
    }
  }

  def partPath(path: String, partFile: String): String = path + "/parts/" + partFile

  def writeSingle(
    execCtx: ExecuteContext,
    path: String,
    rowType: PStruct,
    bufferSpec: BufferSpec,
    rows: IndexedSeq[Annotation]
  ): Array[Long] = {
    val fs = execCtx.fs
    val partsPath = path + "/parts"
    fs.mkDir(partsPath)

    val filePath = if (TaskContext.get == null)
      "part-0"
    else
      partFile(0, 0, TaskContext.get)
    val codecSpec = TypedCodecSpec(rowType, bufferSpec)

    val part0Count =
      using(fs.create(partsPath + "/" + filePath)) { os =>
        using(RVDContext.default) { ctx =>
          val rvb = ctx.rvb
          RichContextRDDRegionValue.writeRowsPartition(codecSpec.buildEncoder(execCtx, rowType))(ctx,
            rows.iterator.map { a =>
              rvb.start(rowType)
              rvb.addAnnotation(rowType.virtualType, a)
              rvb.end()
            }, os, null)
        }
      }

    val spec = MakeRVDSpec(FastIndexedSeq(), codecSpec, Array(filePath), RVDPartitioner.unkeyed(1))
    spec.write(fs, path)

    Array(part0Count)
  }

  def readZipped(
    ctx: ExecuteContext,
    specLeft: AbstractRVDSpec,
    specRight: AbstractRVDSpec,
    pathLeft: String,
    pathRight: String,
    requestedType: TStruct,
    requestedTypeLeft: TStruct,
    requestedTypeRight: TStruct,
    newPartitioner: Option[RVDPartitioner],
    filterIntervals: Boolean
  ): RVD = {
    require(specRight.key.isEmpty)
    require(requestedType == requestedTypeLeft ++ requestedTypeRight)
    val requestedKey = specLeft.key.takeWhile(requestedTypeLeft.hasField)
    val partitioner = specLeft.partitioner

    val extendedNewPartitioner = newPartitioner.map(_.extendKey(partitioner.kType))
    val tmpPartitioner = extendedNewPartitioner match {
      case Some(np) => np.intersect(partitioner)
      case None => partitioner
    }

    val parts = if (specLeft.key.isEmpty)
      specLeft.partFiles
    else
      tmpPartitioner.rangeBounds.map { b => specLeft.partFiles(partitioner.lowerBoundInterval(b)) }

    val (isl, isr) = (specLeft, specRight) match {
      case (l: Indexed, r: Indexed) => (Some(l.indexSpec), Some(r.indexSpec))
      case _ => (None, None)
    }

    val (t, crdd) = HailContext.readRowsSplit(ctx,
      pathLeft, pathRight, isl, isr,
      specLeft.typedCodecSpec, specRight.typedCodecSpec, parts, tmpPartitioner.rangeBounds,
      requestedTypeLeft, requestedTypeRight)
    assert(t.virtualType == requestedType)
    val tmprvd = RVD(RVDType(t, requestedKey), tmpPartitioner.coarsen(requestedKey.length), crdd)
    extendedNewPartitioner match {
      case Some(part) if !filterIntervals => tmprvd.repartition(ctx, part.coarsen(requestedKey.length))
      case _ => tmprvd
    }
  }
}

trait Indexed extends AbstractRVDSpec {
  override val indexed: Boolean = true

  def indexSpec: AbstractIndexSpec
}

abstract class AbstractRVDSpec {
  def partitioner: RVDPartitioner

  def key: IndexedSeq[String]

  def partFiles: Array[String]

  def absolutePartPaths(path: String): Array[String] = partFiles.map(path + "/parts/" + _)

  def typedCodecSpec: AbstractTypedCodecSpec

  def indexed: Boolean = false

  def attrs: Map[String, String]

  def read(
    ctx: ExecuteContext,
    path: String,
    requestedType: TStruct,
    newPartitioner: Option[RVDPartitioner] = None,
    filterIntervals: Boolean = false
  ): RVD = newPartitioner match {
    case Some(_) => fatal("attempted to read unindexed data as indexed")
    case None =>
      val requestedKey = key.takeWhile(requestedType.hasField)
      val (pType: PStruct, crdd) = HailContext.readRows(ctx, path, typedCodecSpec, partFiles, requestedType)
      val rvdType = RVDType(pType, requestedKey)

      RVD(rvdType, partitioner.coarsen(requestedKey.length), crdd)
  }

  def readLocalSingleRow(ctx: ExecuteContext, path: String, requestedType: TStruct): (PStruct, Long) =
    AbstractRVDSpec.readLocal(ctx, path, typedCodecSpec, partFiles, requestedType)

  def write(fs: FS, path: String) {
    using(fs.create(path + "/metadata.json.gz")) { out =>
      import AbstractRVDSpec.formats
      Serialization.write(this, out)
    }
  }
}

trait AbstractIndexSpec {
  def relPath: String

  def leafCodec: AbstractTypedCodecSpec

  def internalNodeCodec: AbstractTypedCodecSpec

  def keyType: Type

  def annotationType: Type

  def offsetField: Option[String] = None

  def types: (Type, Type) = (keyType, annotationType)
}

case class IndexSpec2(_relPath: String,
  _leafCodec: AbstractTypedCodecSpec,
  _internalNodeCodec: AbstractTypedCodecSpec,
  _keyType: Type,
  _annotationType: Type,
  _offsetField: Option[String] = None
) extends AbstractIndexSpec {
  def relPath: String = _relPath

  def leafCodec: AbstractTypedCodecSpec = _leafCodec

  def internalNodeCodec: AbstractTypedCodecSpec = _internalNodeCodec

  def keyType: Type = _keyType

  def annotationType: Type = _annotationType

  override def offsetField: Option[String] = _offsetField
}


object IndexSpec {
  def fromKeyAndValuePTypes(relPath: String, keyPType: PType, annotationPType: PType, offsetFieldName: Option[String]): AbstractIndexSpec = {
    val leafType = LeafNodeBuilder.typ(keyPType, annotationPType)
    val leafNodeSpec = TypedCodecSpec(leafType, BufferSpec.default)
    val internalType = InternalNodeBuilder.typ(keyPType, annotationPType)
    val internalNodeSpec = TypedCodecSpec(internalType, BufferSpec.default)
    IndexSpec2(relPath, leafNodeSpec, internalNodeSpec, keyPType.virtualType, annotationPType.virtualType, offsetFieldName)
  }

  def emptyAnnotation(relPath: String, keyType: PStruct): AbstractIndexSpec = {
    fromKeyAndValuePTypes(relPath, keyType, PCanonicalStruct(required = true), None)
  }

  def defaultAnnotation(relPath: String, keyType: PStruct, withOffsetField: Boolean = false): AbstractIndexSpec = {
    val name = "entries_offset"
    fromKeyAndValuePTypes(relPath, keyType, PCanonicalStruct(required = true, name -> PInt64Optional),
      if (withOffsetField) Some(name) else None)
  }
}

object MakeRVDSpec {
  def apply(
    key: IndexedSeq[String],
    codecSpec: AbstractTypedCodecSpec,
    partFiles: Array[String],
    partitioner: RVDPartitioner,
    indexSpec: AbstractIndexSpec = null,
    attrs: Map[String, String] = Map.empty
  ): AbstractRVDSpec = {
    val partJV = JSONAnnotationImpex.exportAnnotation(
      partitioner.rangeBounds.toFastSeq,
      partitioner.rangeBoundsType)
    Option(indexSpec) match {
      case Some(ais) => IndexedRVDSpec2(
        key,
        codecSpec,
        ais,
        partFiles,
        partJV,
        attrs)
      case None => OrderedRVDSpec2(
        key,
        codecSpec,
        partFiles,
        partJV,
        attrs
      )
    }
  }
}

object IndexedRVDSpec2 {
  def apply(key: IndexedSeq[String],
    codecSpec: AbstractTypedCodecSpec,
    indexSpec: AbstractIndexSpec,
    partFiles: Array[String],
    partitioner: RVDPartitioner,
    attrs: Map[String, String]
  ): AbstractRVDSpec = {
    IndexedRVDSpec2(
      key,
      codecSpec,
      indexSpec,
      partFiles,
      JSONAnnotationImpex.exportAnnotation(
        partitioner.rangeBounds.toFastSeq,
        partitioner.rangeBoundsType),
      attrs)
  }
}

case class IndexedRVDSpec2(_key: IndexedSeq[String],
  _codecSpec: AbstractTypedCodecSpec,
  _indexSpec: AbstractIndexSpec,
  _partFiles: Array[String],
  _jRangeBounds: JValue,
  _attrs: Map[String, String]) extends AbstractRVDSpec with Indexed {

  // some lagacy OrderedRVDSpec2 were written out without the toplevel encoder required
  private val codecSpec2 = _codecSpec match {
    case cs: TypedCodecSpec =>
      TypedCodecSpec(cs._eType.setRequired(true), cs._vType, cs._bufferSpec)
  }

  require(codecSpec2.encodedType.required)

  def typedCodecSpec: AbstractTypedCodecSpec = codecSpec2

  def indexSpec: AbstractIndexSpec = _indexSpec

  lazy val partitioner: RVDPartitioner = {
    val keyType = codecSpec2.encodedVirtualType.asInstanceOf[TStruct].select(key)._1
    val rangeBoundsType = TArray(TInterval(keyType))
    new RVDPartitioner(keyType,
      JSONAnnotationImpex.importAnnotation(_jRangeBounds, rangeBoundsType, padNulls = false).asInstanceOf[IndexedSeq[Interval]])
  }

  def partFiles: Array[String] = _partFiles

  def key: IndexedSeq[String] = _key

  val attrs: Map[String, String] = _attrs

  override def read(
    ctx: ExecuteContext,
    path: String,
    requestedType: TStruct,
    newPartitioner: Option[RVDPartitioner] = None,
    filterIntervals: Boolean = false
  ): RVD = {
    newPartitioner match {
      case Some(np) =>
        val extendedNP = np.extendKey(partitioner.kType)
        val requestedKey = key.takeWhile(requestedType.hasField)
        val tmpPartitioner = partitioner.intersect(extendedNP)

        assert(key.nonEmpty)
        val parts = tmpPartitioner.rangeBounds.map { b => partFiles(partitioner.lowerBoundInterval(b)) }

        val (decPType: PStruct, crdd) = HailContext.readIndexedRows(ctx, path, _indexSpec, typedCodecSpec, parts, tmpPartitioner.rangeBounds, requestedType)
        val rvdType = RVDType(decPType, requestedKey)
        val tmprvd = RVD(rvdType, tmpPartitioner.coarsen(requestedKey.length), crdd)

        if (filterIntervals)
          tmprvd
        else
          tmprvd.repartition(ctx, extendedNP.coarsen(requestedKey.length))
      case None =>
        // indexed reads are costly; don't use an indexed read when possible
        super.read(ctx, path, requestedType, None, filterIntervals)
    }
  }
}

case class OrderedRVDSpec2(_key: IndexedSeq[String],
  _codecSpec: AbstractTypedCodecSpec,
  _partFiles: Array[String],
  _jRangeBounds: JValue,
  _attrs: Map[String, String]) extends AbstractRVDSpec {

  // some lagacy OrderedRVDSpec2 were written out without the toplevel encoder required
  private val codecSpec2 = _codecSpec match {
    case cs: TypedCodecSpec =>
      TypedCodecSpec(cs._eType.setRequired(true), cs._vType, cs._bufferSpec)
  }

  require(codecSpec2.encodedType.required)

  lazy val partitioner: RVDPartitioner = {
    val keyType = codecSpec2.encodedVirtualType.asInstanceOf[TStruct].select(key)._1
    val rangeBoundsType = TArray(TInterval(keyType))
    new RVDPartitioner(keyType,
      JSONAnnotationImpex.importAnnotation(_jRangeBounds, rangeBoundsType, padNulls = false).asInstanceOf[IndexedSeq[Interval]])
  }

  def partFiles: Array[String] = _partFiles

  def key: IndexedSeq[String] = _key

  val attrs: Map[String, String] = _attrs

  def typedCodecSpec: AbstractTypedCodecSpec = codecSpec2
}
