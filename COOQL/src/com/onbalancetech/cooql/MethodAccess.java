/**
 * Copyright (c) 2008, Nathan Sweet All rights reserved.
 * Modified, repackaged and stripped down 2014, Jason Kania
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * disclaimer. 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 * the following disclaimer in the documentation and/or other materials provided with the distribution. 3. Neither the
 * name of Esoteric Software nor the names of its contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * 
 */

package com.onbalancetech.cooql;

import static org.objectweb.asm.Opcodes.*;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public abstract class MethodAccess
{
	private ConcurrentHashMap<String,MethodDescriptor> methodMap;

	private static class MethodDescriptor
	{
		String methodName;
		Class parameterType;
		Class returnType;
		int index;
	}

	abstract public Object invoke( Object object, int methodIndex, Object... args );

	public Object invoke( Object object, String methodName, Object... args )
	{
		return invoke( object, methodMap.get( methodName ).index, args);
	}

	public List<String> getGetterMethodNameList( List<String> getterMethodNameList )
	{
		getterMethodNameList.clear();
		Iterator<String> iterator = methodMap.keySet().iterator();
		String methodName;
		while (iterator.hasNext())
		{
			methodName = iterator.next();
			if (methodName.startsWith( "get" ))
			{
				getterMethodNameList.add( methodName );
			}
		}

		return getterMethodNameList;
	}

	public String getMatchingSetter( String getterMethodName )
	{
		String setterMethodName = "set" + getterMethodName.substring( 3 );
		MethodDescriptor methodDescriptor = methodMap.get( setterMethodName );
		if (methodDescriptor == null) return null;
		return methodMap.get( setterMethodName ).methodName;
	}

	public Class getParameterType( String setterMethodName )
	{
		return methodMap.get( setterMethodName ).parameterType;
	}

	public List<String> getSetterMethodNameList( List<String> setterMethodNameList )
	{
		setterMethodNameList.clear();
		Iterator<String> iterator = methodMap.keySet().iterator();
		String methodName;
		while (iterator.hasNext())
		{
			methodName = iterator.next();
			if (methodName.startsWith( "set" ))
			{
				setterMethodNameList.add( methodName );
			}
		}

		return setterMethodNameList;
	}

	static public MethodAccess get( Class type )
	{
		ArrayList<Method> methods = new ArrayList<Method>();
		boolean isInterface = type.isInterface();
		if (!isInterface)
		{
			Class nextClass = type;
			while (nextClass != Object.class)
			{
				XaddAccessorMethodsToList( nextClass, methods );
				nextClass = nextClass.getSuperclass();
			}
		}
		else XrecursiveAddInterfaceMethodsToList( type, methods );

		int n = methods.size();
		ConcurrentHashMap<String,MethodDescriptor> methodMap = new ConcurrentHashMap<String,MethodDescriptor>( 20 );
		String[] methodNames = new String[ n ];
		Method method;
		MethodDescriptor methodDescriptor;
		for (int i = 0; i<n; i++)
		{
			method = methods.get( i );
			methodDescriptor = new MethodDescriptor();
			methodNames[ i ] = methodDescriptor.methodName = method.getName();
			if (method.getParameterTypes().length > 0) methodDescriptor.parameterType = method.getParameterTypes()[ 0 ];
			methodDescriptor.returnType = method.getReturnType();
			methodDescriptor.index = i;
			methodMap.put( methodDescriptor.methodName, methodDescriptor );
		}

		String className = type.getName();
		String accessClassName = className+"MethodAccess";
		Class accessClass;

		AccessClassLoader loader = AccessClassLoader.get( type );
		synchronized (loader)
		{
			try
			{
				accessClass = loader.loadClass( accessClassName );
			}
			catch (ClassNotFoundException ignored)
			{
				String accessClassNameInternal = accessClassName.replace( '.', '/' );
				String classNameInternal = className.replace( '.', '/' );

				ClassWriter cw = new ClassWriter( ClassWriter.COMPUTE_MAXS );
				MethodVisitor visitor;
				cw.visit( V1_1, ACC_PUBLIC+ACC_SUPER, accessClassNameInternal, null,
						"com/onbalancetech/cooql/MethodAccess", null );
				{
					visitor = cw.visitMethod( ACC_PUBLIC, "<init>", "()V", null, null );
					visitor.visitCode();
					visitor.visitVarInsn( ALOAD, 0 );
					visitor.visitMethodInsn( INVOKESPECIAL, "com/onbalancetech/cooql/MethodAccess", "<init>", "()V" );
					visitor.visitInsn( RETURN );
					visitor.visitMaxs( 0, 0 );
					visitor.visitEnd();
				}
				{
					visitor = cw.visitMethod( ACC_PUBLIC+ACC_VARARGS, "invoke",
									"(Ljava/lang/Object;I[Ljava/lang/Object;)Ljava/lang/Object;", null, null );
					visitor.visitCode();

					if (!methods.isEmpty())
					{
						visitor.visitVarInsn( ALOAD, 1 );
						visitor.visitTypeInsn( CHECKCAST, classNameInternal );
						visitor.visitVarInsn( ASTORE, 4 );

						visitor.visitVarInsn( ILOAD, 2 );
						Label[] labels = new Label[n];
						for (int i = 0; i<n; i++)
						{
							labels[ i ] = new Label();
						}
						Label defaultLabel = new Label();
						visitor.visitTableSwitchInsn( 0, labels.length-1, defaultLabel, labels );

						StringBuilder buffer = new StringBuilder( 128 );
						for (int i = 0; i<n; i++)
						{
							visitor.visitLabel( labels[ i ] );
							if (i==0) visitor.visitFrame( Opcodes.F_APPEND, 1, new Object[] { classNameInternal }, 0, null );
							else visitor.visitFrame( Opcodes.F_SAME, 0, null, 0, null );
							visitor.visitVarInsn( ALOAD, 4 );

							buffer.setLength( 0 );
							buffer.append( '(' );

							String methodName = methodNames[ i ];
							if (methodMap.get( methodName ).parameterType != null)
							{
								Type paramType = Type.getType( methodMap.get( methodName ).parameterType );

								visitor.visitVarInsn( ALOAD, 3 );
								visitor.visitIntInsn( BIPUSH, 0 );
								visitor.visitInsn( AALOAD );
								switch (paramType.getSort())
								{
									case Type.BOOLEAN:
										visitor.visitTypeInsn( CHECKCAST, "java/lang/Boolean" );
										visitor.visitMethodInsn( INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue", "()Z" );
										break;
									case Type.BYTE:
										visitor.visitTypeInsn( CHECKCAST, "java/lang/Byte" );
										visitor.visitMethodInsn( INVOKEVIRTUAL, "java/lang/Byte", "byteValue", "()B" );
										break;
									case Type.CHAR:
										visitor.visitTypeInsn( CHECKCAST, "java/lang/Character" );
										visitor.visitMethodInsn( INVOKEVIRTUAL, "java/lang/Character", "charValue", "()C" );
										break;
									case Type.SHORT:
										visitor.visitTypeInsn( CHECKCAST, "java/lang/Short" );
										visitor.visitMethodInsn( INVOKEVIRTUAL, "java/lang/Short", "shortValue", "()S" );
										break;
									case Type.INT:
										visitor.visitTypeInsn( CHECKCAST, "java/lang/Integer" );
										visitor.visitMethodInsn( INVOKEVIRTUAL, "java/lang/Integer", "intValue", "()I" );
										break;
									case Type.FLOAT:
										visitor.visitTypeInsn( CHECKCAST, "java/lang/Float" );
										visitor.visitMethodInsn( INVOKEVIRTUAL, "java/lang/Float", "floatValue", "()F" );
										break;
									case Type.LONG:
										visitor.visitTypeInsn( CHECKCAST, "java/lang/Long" );
										visitor.visitMethodInsn( INVOKEVIRTUAL, "java/lang/Long", "longValue", "()J" );
										break;
									case Type.DOUBLE:
										visitor.visitTypeInsn( CHECKCAST, "java/lang/Double" );
										visitor.visitMethodInsn( INVOKEVIRTUAL, "java/lang/Double", "doubleValue", "()D" );
										break;
									case Type.ARRAY:
										visitor.visitTypeInsn( CHECKCAST, paramType.getDescriptor() );
										break;
									case Type.OBJECT:
										visitor.visitTypeInsn( CHECKCAST, paramType.getInternalName() );
										break;
								}
								buffer.append( paramType.getDescriptor() );
							}
						
							Class returnType = methodMap.get( methodName ).returnType;
							buffer.append( ')' );
							buffer.append( Type.getDescriptor( returnType ) );
							visitor.visitMethodInsn( isInterface?INVOKEINTERFACE:INVOKEVIRTUAL, classNameInternal,
									methodName, buffer.toString() );

							switch (Type.getType( returnType ).getSort())
							{
								case Type.VOID:
									visitor.visitInsn( ACONST_NULL );
									break;
								case Type.BOOLEAN:
									visitor.visitMethodInsn( INVOKESTATIC, "java/lang/Boolean", "valueOf", "(Z)Ljava/lang/Boolean;" );
									break;
								case Type.BYTE:
									visitor.visitMethodInsn( INVOKESTATIC, "java/lang/Byte", "valueOf", "(B)Ljava/lang/Byte;" );
									break;
								case Type.CHAR:
									visitor.visitMethodInsn( INVOKESTATIC, "java/lang/Character", "valueOf", "(C)Ljava/lang/Character;" );
									break;
								case Type.DOUBLE:
									visitor.visitMethodInsn( INVOKESTATIC, "java/lang/Double", "valueOf", "(D)Ljava/lang/Double;" );
									break;
								case Type.FLOAT:
									visitor.visitMethodInsn( INVOKESTATIC, "java/lang/Float", "valueOf", "(F)Ljava/lang/Float;" );
									break;
								case Type.INT:
									visitor.visitMethodInsn( INVOKESTATIC, "java/lang/Integer", "valueOf", "(I)Ljava/lang/Integer;" );
									break;
								case Type.LONG:
									visitor.visitMethodInsn( INVOKESTATIC, "java/lang/Long", "valueOf", "(J)Ljava/lang/Long;" );
									break;
								case Type.SHORT:
									visitor.visitMethodInsn( INVOKESTATIC, "java/lang/Short", "valueOf", "(S)Ljava/lang/Short;" );
									break;
							}

							visitor.visitInsn( ARETURN );
						}

						visitor.visitLabel( defaultLabel );
						visitor.visitFrame( Opcodes.F_SAME, 0, null, 0, null );
					}
					visitor.visitTypeInsn( NEW, "java/lang/IllegalArgumentException" );
					visitor.visitInsn( DUP );
					visitor.visitTypeInsn( NEW, "java/lang/StringBuilder" );
					visitor.visitInsn( DUP );
					visitor.visitLdcInsn( "Method not found: " );
					visitor.visitMethodInsn( INVOKESPECIAL, "java/lang/StringBuilder", "<init>", "(Ljava/lang/String;)V" );
					visitor.visitVarInsn( ILOAD, 2 );
					visitor.visitMethodInsn( INVOKEVIRTUAL, "java/lang/StringBuilder", "append", "(I)Ljava/lang/StringBuilder;" );
					visitor.visitMethodInsn( INVOKEVIRTUAL, "java/lang/StringBuilder", "toString", "()Ljava/lang/String;" );
					visitor.visitMethodInsn( INVOKESPECIAL, "java/lang/IllegalArgumentException", "<init>", "(Ljava/lang/String;)V" );
					visitor.visitInsn( ATHROW );
					visitor.visitMaxs( 0, 0 );
					visitor.visitEnd();
				}
				cw.visitEnd();
				byte[] data = cw.toByteArray();
				accessClass = loader.defineClass( accessClassName, data );
			}
		}

		try
		{
			MethodAccess access = (MethodAccess)accessClass.newInstance();
			access.methodMap = methodMap;
			return access;
		}
		catch (Throwable t)
		{
			throw new RuntimeException( "Error constructing method accessor class: "+accessClassName, t );
		}
	}

	private static void XaddAccessorMethodsToList( Class type, ArrayList<Method> methods )
	{
		int modifiers;
		Method[] methodList = type.getDeclaredMethods();
		Method method;

		for (int i = 0, n = methodList.length; i<n; i++)
		{
			method = methodList[ i ];
			modifiers = method.getModifiers();
			if ((Modifier.isStatic( modifiers )) || (Modifier.isPrivate( modifiers ))) continue;
			if ((method.getName().startsWith( "get" )) && (method.getParameterTypes().length == 0) && (method.getReturnType() != null))
			{
				if (!XisUnderstoodType( method.getReturnType() )) continue;
			}
			else if ((method.getName().startsWith( "set" )) && (method.getParameterTypes().length == 1) && (method.getReturnType() == void.class))
			{
				if (!XisUnderstoodType( method.getParameterTypes()[ 0 ] )) continue;
			}
			else continue;
			methods.add( method );
		}
	}

	private static boolean XisUnderstoodType( Class aType )
	{
		if ((aType.equals( Boolean.class )) || (aType == boolean.class)) {}
		else if ((aType.equals( Byte.class )) || (aType == byte.class)) {}
		else if (ByteBuffer.class.isAssignableFrom( aType ) ) {}
		else if (aType.equals( Date.class )) {}
		else if ((aType.equals( Double.class )) || (aType == double.class)) {}
		else if ((aType.equals( Float.class )) || (aType == float.class)) {}
		else if ((aType.equals( Integer.class )) || (aType == int.class)) {}
		else if (List.class.isAssignableFrom( aType )) {}
		else if ((aType.equals( Long.class )) || (aType == long.class)) {}
		else if (Map.class.isAssignableFrom( aType )) {}
		else if (Set.class.isAssignableFrom( aType )) {}
		else if ((aType.equals( Short.class )) || (aType == short.class)) {}
		else if ((aType.equals( String.class ))) {}
		else return false;

		return true;
	}

	private static void XrecursiveAddInterfaceMethodsToList( Class interfaceType, ArrayList<Method> methods )
	{
		XaddAccessorMethodsToList( interfaceType, methods );
		for (Class nextInterface : interfaceType.getInterfaces())
		{
			XrecursiveAddInterfaceMethodsToList( nextInterface, methods );
		}
	}
}
