package is.hail.expr.ir.agg

import is.hail.annotations.{CodeOrdering, Region, StagedRegionValueBuilder}
import is.hail.asm4s._
import is.hail.expr.ir.{EmitClassBuilder, EmitCode, EmitCodeBuilder, EmitRegion}
import is.hail.types.encoded.EType
import is.hail.types.physical._
import is.hail.io._
import is.hail.types.VirtualTypeWithReq
import is.hail.types.virtual.Type
import is.hail.utils._

class TypedKey(typ: PType, kb: EmitClassBuilder[_], region: Value[Region]) extends BTreeKey {
  val inline: Boolean = typ.isPrimitive
  val storageType: PTuple = PCanonicalTuple(false, if (inline) typ else PInt64(typ.required), PCanonicalTuple(false))
  val compType: PType = typ
  private val kcomp = kb.getCodeOrdering(typ, CodeOrdering.Compare(), ignoreMissingness = false)

  def isKeyMissing(src: Code[Long]): Code[Boolean] = storageType.isFieldMissing(src, 0)

  def loadKey(src: Code[Long]): Code[_] = Region.loadIRIntermediate(if (inline) typ else PInt64(typ.required))(storageType.fieldOffset(src, 0))

  def isEmpty(off: Code[Long]): Code[Boolean] = storageType.isFieldMissing(off, 1)

  def initializeEmpty(cb: EmitCodeBuilder, off: Code[Long]): Unit =
    cb += storageType.setFieldMissing(off, 1)

  def store(cb: EmitCodeBuilder, dest: Code[Long], m: Code[Boolean], v: Code[_]): Unit = {
    cb += Code.memoize(dest, "casa_store_dest") { dest =>
      val c = {
        if (typ.isPrimitive)
          Region.storeIRIntermediate(typ)(storageType.fieldOffset(dest, 0), v)
        else
          Region.storeAddress(storageType.fieldOffset(dest, 0), StagedRegionValueBuilder.deepCopyFromOffset(kb, region, typ, coerce[Long](v)))
      }
      if (!typ.required)
        m.mux(
          Code(storageType.setFieldPresent(dest, 1), storageType.setFieldMissing(dest, 0)),
          Code(storageType.stagedInitialize(dest), c))
      else
        Code(storageType.setFieldPresent(dest, 1), c)
    }
  }

  def copy(cb: EmitCodeBuilder, src: Code[Long], dest: Code[Long]): Unit =
    cb += Region.copyFrom(src, dest, storageType.byteSize)

  def deepCopy(cb: EmitCodeBuilder, er: EmitRegion, dest: Code[Long], src: Code[Long]): Unit = {
    if (inline)
      cb += StagedRegionValueBuilder.deepCopy(er, storageType, src, dest)
    else
      cb += Region.storeAddress(dest, StagedRegionValueBuilder.deepCopyFromOffset(er, storageType, src))
  }

  def compKeys(k1: EmitCode, k2: EmitCode): Code[Int] =
    Code(k1.setup, k2.setup, kcomp(k1.m -> k1.v, k2.m -> k2.v))

  def loadCompKey(off: Value[Long]): EmitCode =
    EmitCode(Code._empty, isKeyMissing(off), PCode(typ, loadKey(off)))
}

class AppendOnlySetState(val kb: EmitClassBuilder[_], vt: VirtualTypeWithReq) extends PointerBasedRVAState {
  private val t = vt.canonicalPType
  val root: Settable[Long] = kb.genFieldThisRef[Long]()
  val size: Settable[Int] = kb.genFieldThisRef[Int]()
  val key = new TypedKey(t, kb, region)
  val tree = new AppendOnlyBTree(kb, key, region, root)
  val et = EType.defaultFromPType(t)

  val typ: PStruct = PCanonicalStruct(
    required = true,
    "size" -> PInt32(true),
    "tree" -> PInt64(true))

  override def load(cb: EmitCodeBuilder, regionLoader: (EmitCodeBuilder, Value[Region]) => Unit, srcc: Code[Long]): Unit = {
    super.load(cb, regionLoader, srcc)
    cb.ifx(off.cne(0L),
      {
        cb.assign(size, Region.loadInt(typ.loadField(off, 0)))
        cb.assign(root, Region.loadAddress(typ.loadField(off, 1)))
      })
  }

  override def store(cb: EmitCodeBuilder, regionStorer: (EmitCodeBuilder, Value[Region]) => Unit, destc: Code[Long]): Unit = {
    cb += Region.storeInt(typ.fieldOffset(off, 0), size)
    cb += Region.storeAddress(typ.fieldOffset(off, 1), root)
    super.store(cb, regionStorer, destc)
  }

  def init(cb: EmitCodeBuilder): Unit = {
    cb.assign(off, region.allocate(typ.alignment, typ.byteSize))
    cb.assign(size, 0)
    tree.init(cb)
  }

  private val _elt = kb.genFieldThisRef[Long]()
  private val _v = kb.newEmitField(t)

  def insert(cb: EmitCodeBuilder, v: EmitCode): Unit = {
    cb.assign(_v, v)
    cb.assign(_elt, tree.getOrElseInitialize(cb, _v))
    cb.ifx(key.isEmpty(_elt), {
      cb.assign(size, size + 1)
      key.store(cb, _elt, _v.m, _v.v)
    })
  }

  // loads container; does not update.
  def foreach(cb: EmitCodeBuilder)(f: (EmitCodeBuilder, EmitCode) => Unit): Unit =
    tree.foreach(cb) { (cb, eoffCode) =>
      val eoff = cb.newLocal("casa_foreach_eoff", eoffCode)
      f(cb, EmitCode(Code._empty, key.isKeyMissing(eoff), PCode(t, key.loadKey(eoff))))
    }

  def copyFromAddress(cb: EmitCodeBuilder, srcc: Code[Long]): Unit = {
    val src = cb.newLocal[Long]("aoss_copy_from_addr_src", srcc)
    cb.assign(off, region.allocate(typ.alignment, typ.byteSize))
    cb.assign(size, Region.loadInt(typ.loadField(src, 0)))
    tree.init(cb)
    tree.deepCopy(cb, Region.loadAddress(typ.loadField(src, 1)))
  }

  def serialize(codec: BufferSpec): (EmitCodeBuilder, Value[OutputBuffer]) => Unit = {
    val kEnc = et.buildEncoderMethod(t, kb)

    { (cb: EmitCodeBuilder, ob: Value[OutputBuffer]) =>
      tree.bulkStore(cb, ob) { (cb, ob, srcCode) =>
        val src = cb.newLocal("aoss_ser_src", srcCode)
        cb += ob.writeBoolean(key.isKeyMissing(src))
        cb.ifx(!key.isKeyMissing(src), {
          cb += kEnc.invokeCode(key.loadKey(src), ob)
        })
      }
    }
  }

  def deserialize(codec: BufferSpec): (EmitCodeBuilder, Value[InputBuffer]) => Unit = {
    val kDec = et.buildDecoderMethod(t, kb)
    val km = kb.genFieldThisRef[Boolean]("km")
    val kv = kb.genFieldThisRef("kv")(typeToTypeInfo(t))

    { (cb: EmitCodeBuilder, ib: Value[InputBuffer]) =>
      init(cb)
      tree.bulkLoad(cb, ib) { (cb, ib, dest) =>
        cb.assign(km, ib.readBoolean())
        cb.ifx(!km, {
          cb += kv.storeAny(kDec.invokeCode(region, ib))
        })
        key.store(cb, dest, km, kv)
        cb.assign(size, size + 1)
      }
    }
  }
}

class CollectAsSetAggregator(elem: VirtualTypeWithReq) extends StagedAggregator {
  type State = AppendOnlySetState

  private val elemPType = elem.canonicalPType
  val resultType: PSet = PCanonicalSet(elemPType)
  val initOpTypes: Seq[Type] = Array[Type]()
  val seqOpTypes: Seq[Type] = Array[Type](elem.t)

  protected def _initOp(cb: EmitCodeBuilder, state: State, init: Array[EmitCode]): Unit = {
    assert(init.length == 0)
    state.init(cb)
  }

  protected def _seqOp(cb: EmitCodeBuilder, state: State, seq: Array[EmitCode]): Unit = {
    val Array(elt) = seq
    state.insert(cb, elt)
  }

  protected def _combOp(cb: EmitCodeBuilder, state: State, other: State): Unit =
    other.foreach(cb) { (cb, k) => state.insert(cb, k) }

  protected def _result(cb: EmitCodeBuilder, state: State, srvb: StagedRegionValueBuilder): Unit =
    cb += srvb.addArray(resultType.arrayFundamentalType, { sab =>
      EmitCodeBuilder.scopedVoid(cb.emb) { cb =>
        cb += sab.start(state.size)
        state.foreach(cb) { (cb, k) =>
          k.toI(cb)
            .consume(cb,
              cb += sab.setMissing(),
              pv => cb += sab.addIRIntermediate(pv, deepCopy = true))
          cb += sab.advance()
        }
      }
    })
}
